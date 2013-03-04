//$Id: OperationsImpl.java 7844 2008-11-21 13:00:41Z gertsp $
/*
 * <p><b>License and Copyright: </b>The contents of this file is subject to the
 * same open source license as the Fedora Repository System at www.fedora-commons.org
 * Copyright &copy; 2006, 2007, 2008 by The Technical University of Denmark.
 * All rights reserved.</p>
 */
package dk.defxws.fgslucene;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.rmi.RemoteException;
import java.util.ListIterator;
import java.util.StringTokenizer;

import javax.xml.transform.stream.StreamSource;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

import dk.defxws.fedoragsearch.server.GTransformer;
import dk.defxws.fedoragsearch.server.GenericOperationsImpl;
import dk.defxws.fedoragsearch.server.errors.GenericSearchException;
import dk.defxws.fedoragsearch.server.utils.IOUtils;
import dk.defxws.fedoragsearch.server.utils.Stream;

/**
 * performs the Lucene specific parts of the operations
 * 
 * @author  gsp@dtv.dk
 * @version 
 */
public class OperationsImpl extends GenericOperationsImpl {
    
    private static final Logger logger = Logger.getLogger(OperationsImpl.class);
    
    private IndexWriter iw = null;
    private IndexReader ir = null;
    
    public String gfindObjects(
            String query,
            int hitPageStart,
            int hitPageSize,
            int snippetsMax,
            int fieldMaxLength,
            String indexName,
            String sortFields,
            String resultPageXslt)
    throws java.rmi.RemoteException {
        super.gfindObjects(query, hitPageStart, hitPageSize, snippetsMax, fieldMaxLength, indexName, sortFields, resultPageXslt);
        String usingIndexName = config.getIndexName(indexName);
        if (srf != null && config.isSearchResultFilteringActive("presearch")) {
        	usingIndexName = srf.selectIndexNameForPresearch(fgsUserName, usingIndexName);
            if (logger.isDebugEnabled())
                logger.debug("gfindObjects presearch" +
                        " fgsUserName="+fgsUserName+
                        " usingIndexName="+usingIndexName);
        }
        String usingQuery = query;
        if (srf != null && config.isSearchResultFilteringActive("insearch")) {
        	usingQuery = srf.rewriteQueryForInsearch(fgsUserName, usingIndexName, query);
            if (logger.isDebugEnabled())
                logger.debug("gfindObjects insearch" +
                        " fgsUserName="+fgsUserName+
                        " usingQuery="+usingQuery);
        }
        ResultSet resultSet = (new Connection()).createStatement().executeQuery(
        		usingQuery,
                hitPageStart,
                hitPageSize,
                snippetsMax,
                fieldMaxLength,
                getQueryAnalyzer(usingIndexName),
                config.getDefaultQueryFields(usingIndexName),
                config.getIndexDir(usingIndexName),
                usingIndexName,
                config.getSnippetBegin(usingIndexName),
                config.getSnippetEnd(usingIndexName),
                config.getSortFields(usingIndexName, sortFields));
        params[12] = "RESULTPAGEXSLT";
        params[13] = resultPageXslt;
        String xsltPath = config.getConfigName()+"/index/"+usingIndexName+"/"+config.getGfindObjectsResultXslt(usingIndexName, resultPageXslt);
        Stream stream = new GTransformer().transform(
        		xsltPath,
        		resultSet.getResultXml(),
                params);
        StringBuffer resultXml = IOUtils.convertStreamToStringBuffer(stream);
        if (srf != null && config.isSearchResultFilteringActive("postsearch")) {
        	resultXml = srf.filterResultsetForPostsearch(fgsUserName, resultXml, config);
            if (logger.isDebugEnabled())
                logger.debug("gfindObjects postsearch" +
                        " fgsUserName="+fgsUserName+
                        " resultXml=\n"+resultXml);
        }
        return resultXml.toString();
    }

    public String getIndexInfo(
            String indexName,
            String resultPageXslt)
    throws java.rmi.RemoteException {
        super.getIndexInfo(indexName, resultPageXslt);
        InputStream infoStream =  null;
        String indexInfoPath = "/"+config.getConfigName()+"/index/"+config.getIndexName(indexName)+"/indexInfo.xml";
        try {
            infoStream =  OperationsImpl.class.getResourceAsStream(indexInfoPath);
            if (infoStream == null) {
                throw new GenericSearchException("Error "+indexInfoPath+" not found in classpath");
            }
        } catch (IOException e) {
            throw new GenericSearchException("Error "+indexInfoPath+" not found in classpath", e);
        }
        String xsltPath = config.getConfigName()+"/index/"+config.getIndexName(indexName)+"/"+config.getIndexInfoResultXslt(indexName, resultPageXslt);
        Stream stream = (new GTransformer()).transform(
        		xsltPath,
                new StreamSource(infoStream),
                new String[] {});
        StringBuffer sb = IOUtils.convertStreamToStringBuffer(stream);
        return sb.toString();
    }
    
    public String updateIndex(
            String action,
            String value,
            String repositoryName,
            String indexName,
            String indexDocXslt,
            String resultPageXslt)
    throws java.rmi.RemoteException {
        insertTotal = 0;
        updateTotal = 0;
        deleteTotal = 0;
        int initDocCount = 0;
        StringBuffer resultXml = new StringBuffer(); 
        resultXml.append("<luceneUpdateIndex");
        resultXml.append(" indexName=\""+indexName+"\">\n");

        try {
        	if ("createEmpty".equals(action)) 
        		createEmpty(indexName, resultXml);
        	else {
        		initDocCount = docCount;
        		if ("deletePid".equals(action)) { 
        			deletePid(value, indexName, resultXml);
        		}
        		else {
        			if ("fromPid".equals(action)) {
						fromPid(value, repositoryName, indexName, resultXml, indexDocXslt);
        			}
        			else {
        				if ("fromFoxmlFiles".equals(action)) { 
        					fromFoxmlFiles(value, repositoryName, indexName, resultXml, indexDocXslt);
        				}
        				else if ("optimize".equals(action)) { 
                			optimize(indexName, resultXml);
        				}
        			}
        		}
        	}
        } finally {
        	if (updateTotal > 0) {
        		int diff = docCount - initDocCount;
        		insertTotal = diff;
        		updateTotal -= diff;
        	}
        }
        logger.info("updateIndex "+action+" indexName="+indexName
        		+" indexDirSpace="+indexDirSpace(new File(config.getIndexDir(indexName)))
        		+" docCount="+docCount);
        resultXml.append("<counts");
        resultXml.append(" insertTotal=\""+insertTotal+"\"");
        resultXml.append(" updateTotal=\""+updateTotal+"\"");
        resultXml.append(" deleteTotal=\""+deleteTotal+"\"");
        resultXml.append(" docCount=\""+docCount+"\"");
        resultXml.append(" warnCount=\""+warnCount+"\"");
        resultXml.append("/>\n");
        resultXml.append("</luceneUpdateIndex>\n");
        params = new String[12];
        params[0] = "OPERATION";
        params[1] = "updateIndex";
        params[2] = "ACTION";
        params[3] = action;
        params[4] = "VALUE";
        params[5] = value;
        params[6] = "REPOSITORYNAME";
        params[7] = repositoryName;
        params[8] = "INDEXNAME";
        params[9] = indexName;
        params[10] = "RESULTPAGEXSLT";
        params[11] = resultPageXslt;
        String xsltPath = config.getConfigName()+"/index/"+config.getIndexName(indexName)+"/"+config.getUpdateIndexResultXslt(indexName, resultPageXslt);
        Stream stream = new GTransformer().transform(xsltPath, resultXml, params);
        StringBuffer sb = IOUtils.convertStreamToStringBuffer(stream);
        return sb.toString();
    }
    
    private void createEmpty(
            String indexName,
            StringBuffer resultXml)
    throws java.rmi.RemoteException {
        IndexWriterCache.getInstance().createEmpty(indexName, config);
        resultXml.append("<createEmpty/>\n");
    }
    
    private void deletePid(
            String pid,
            String indexName,
            StringBuffer resultXml)
    throws java.rmi.RemoteException {
        IndexWriterCache.getInstance().delete(indexName, config, pid, true);
        deleteTotal = 1;
        resultXml.append("<deletePid pid=\""+pid+"\"/>\n");
    }
    
    private void optimize(
            String indexName,
    		StringBuffer resultXml)
    throws java.rmi.RemoteException {
        IndexWriterCache.getInstance().optimize(indexName, config);
        resultXml.append("<optimize/>\n");
    }
    
    private void fromFoxmlFiles(
            String filePath,
            String repositoryName,
            String indexName,
            StringBuffer resultXml,
            String indexDocXslt)
    throws java.rmi.RemoteException {
        if (logger.isDebugEnabled())
            logger.debug("fromFoxmlFiles filePath="+filePath+" repositoryName="+repositoryName+" indexName="+indexName);
        File objectDir = null;
        if (filePath==null || filePath.isEmpty()) {
            objectDir = config.getFedoraObjectDir(repositoryName);
        }
        else {
        	objectDir = new File(filePath);
        }
        indexDocs(objectDir, repositoryName, indexName, resultXml, indexDocXslt);
        docCount = docCount-warnCount;
        resultXml.append("<warnCount>"+warnCount+"</warnCount>\n");
        resultXml.append("<docCount>"+docCount+"</docCount>\n");
    }
    
    private void indexDocs(
            File file, 
            String repositoryName,
            String indexName,
            StringBuffer resultXml, 
            String indexDocXslt)
    throws java.rmi.RemoteException
    {
		if (file.isHidden()) return;
        if (file.isDirectory())
        {
            String[] files = file.list();
            for (int i = 0; i < files.length; i++) {
                if (i % 100 == 0)
                    logger.info("updateIndex fromFoxmlFiles "+file.getAbsolutePath()
                    		+" indexDirSpace="+indexDirSpace(new File(config.getIndexDir(indexName)))
                    		+" docCount="+docCount);
                indexDocs(new File(file, files[i]), repositoryName, indexName, resultXml, indexDocXslt);
            }
        }
        else
        {
            try {
                indexDoc(file.getName(), repositoryName, indexName, new FileInputStream(file), resultXml, indexDocXslt);
            } catch (RemoteException e) {
                resultXml.append("<warning no=\""+(++warnCount)+"\">file="+file.getAbsolutePath()+" exception="+e.toString()+"</warning>\n");
                logger.warn("<warning no=\""+(warnCount)+"\">file="+file.getAbsolutePath()+" exception="+e.toString()+"</warning>");
            } catch (FileNotFoundException e) {
              resultXml.append("<warning no=\""+(++warnCount)+"\">file="+file.getAbsolutePath()+" exception="+e.toString()+"</warning>\n");
              logger.warn("<warning no=\""+(warnCount)+"\">file="+file.getAbsolutePath()+" exception="+e.toString()+"</warning>");
            }
        }
    }
    
    private void fromPid(
            String pid,
            String repositoryName,
            String indexName,
            StringBuffer resultXml,
            String indexDocXslt)
    throws java.rmi.RemoteException {
    	
    	if (pid==null || pid.length()<1) return;
		
    	File tempFile = getFoxmlFromPid(pid, repositoryName);
		FileInputStream ins = null;
		
		try {
			ins = new FileInputStream(tempFile);
		} catch (FileNotFoundException e) {
			throw new java.rmi.RemoteException("Temporary file '" + tempFile + "' not found.", e);
		}
        indexDoc(pid, repositoryName, indexName, ins, resultXml, indexDocXslt);

        if(tempFile != null) {
        	tempFile.delete();
        }
    }
    
    private void indexDoc(
    		String pidOrFilename,
    		String repositoryName,
    		String indexName,
    		InputStream foxmlStream,
    		StringBuffer resultXml,
    		String indexDocXslt)
    throws java.rmi.RemoteException {
    	long time = System.currentTimeMillis();
    	IndexDocumentHandler hdlr = null;
    	String xsltName = indexDocXslt;
    	String[] params = new String[12];
    	int beginParams = indexDocXslt.indexOf("(");
    	if (beginParams > -1) {
    		xsltName = indexDocXslt.substring(0, beginParams).trim();
    		int endParams = indexDocXslt.indexOf(")");
    		if (endParams < beginParams)
    			throw new GenericSearchException("Format error (no ending ')') in indexDocXslt="+indexDocXslt+": ");
    		StringTokenizer st = new StringTokenizer(indexDocXslt.substring(beginParams+1, endParams), ",");
    		params = new String[12+2*st.countTokens()];
    		int i=1; 
    		while (st.hasMoreTokens()) {
    			String param = st.nextToken().trim();
    			if (param==null || param.length()<1)
    				throw new GenericSearchException("Format error (empty param) in indexDocXslt="+indexDocXslt+" params["+i+"]="+param);
    			int eq = param.indexOf("=");
    			if (eq < 0)
    				throw new GenericSearchException("Format error (no '=') in indexDocXslt="+indexDocXslt+" params["+i+"]="+param);
    			String pname = param.substring(0, eq).trim();
    			String pvalue = param.substring(eq+1).trim();
    			if (pname==null || pname.length()<1)
    				throw new GenericSearchException("Format error (no param name) in indexDocXslt="+indexDocXslt+" params["+i+"]="+param);
    			if (pvalue==null || pvalue.length()<1)
                    throw new GenericSearchException("Format error (no param value) in indexDocXslt="+indexDocXslt+" params["+i+"]="+param);
            	params[10+2*i] = pname;
            	params[11+2*i++] = pvalue;
            }
        }
        params[0] = "REPOSITORYNAME";
        params[1] = repositoryName;
        params[2] = "FEDORASOAP";
        params[3] = config.getFedoraSoap(repositoryName);
        params[4] = "FEDORAUSER";
        params[5] = config.getFedoraUser(repositoryName);
        params[6] = "FEDORAPASS";
        params[7] = config.getFedoraPass(repositoryName);
        params[8] = "TRUSTSTOREPATH";
        params[9] = config.getTrustStorePath(repositoryName);
        params[10] = "TRUSTSTOREPASS";
        params[11] = config.getTrustStorePass(repositoryName);
        //MIH: call method getUpdateIndexDocXsltPath
//      String xsltPath = config.getConfigName()+"/index/"+indexName+"/"+config.getUpdateIndexDocXslt(indexName, xsltName);
        String xsltPath = getUpdateIndexDocXsltPath(xsltName);
        if (logger.isDebugEnabled()) {
    		logger.debug("preparing xslt needed " + (System.currentTimeMillis() - time));
            time = System.currentTimeMillis();
        }
    	Stream sb = new GTransformer().transform(
    			xsltPath, 
    			new StreamSource(foxmlStream),
    			config.getURIResolver(indexName),
    			params);
    	if (logger.isDebugEnabled()) {
    		logger.debug("Transformation needed " + (System.currentTimeMillis() - time));
    		//logger.debug("IndexDocument=\n"+sb.toString());
            time = System.currentTimeMillis();
    	}
    	hdlr = new IndexDocumentHandler(
    			this,
    			repositoryName,
    			pidOrFilename,
    			sb);
        if (logger.isDebugEnabled()) {
    		logger.debug("preparing lucene-fields needed " + (System.currentTimeMillis() - time));
            time = System.currentTimeMillis();
        }
    	try {
    		ListIterator li = hdlr.getIndexDocument().getFields().listIterator();
    		if (li.hasNext()) {
    		    IndexWriterCache.getInstance().update(indexName, config, hdlr.getPid(), hdlr.getIndexDocument(), true);
    				updateTotal++;
        			resultXml.append("<updated>"+hdlr.getPid()+"</updated>\n");
    			StringBuffer untokenizedFields = new StringBuffer(config.getUntokenizedFields(indexName));
    			while (li.hasNext()) {
    				Field f = (Field)li.next();
    				if (!f.isTokenized() && f.isIndexed() && untokenizedFields.indexOf(f.name())<0) {
    					untokenizedFields.append(" "+f.name());
    					config.setUntokenizedFields(indexName, untokenizedFields.toString());
    				}
    			}
    			logger.info("IndexDocument="+hdlr.getPid());
    		}
    		else {
    			logger.warn("IndexDocument "+hdlr.getPid()+" does not contain any IndexFields!!! RepositoryName="+repositoryName+" IndexName="+indexName);
    		}
    	} catch (IOException e) {
    		throw new GenericSearchException("Update error pidOrFilename="+pidOrFilename, e);
    	} finally {
            if (logger.isDebugEnabled()) {
        		logger.debug("writing lucene-index needed " + (System.currentTimeMillis() - time));
            }

    	}
    }
    
    public Analyzer getAnalyzer(String analyzerClassName)
    throws GenericSearchException {
        Analyzer analyzer = null;
        if (logger.isDebugEnabled())
            logger.debug("analyzerClassName=" + analyzerClassName);
        try {
            Class analyzerClass = Class.forName(analyzerClassName);
            if (logger.isDebugEnabled())
                logger.debug("analyzerClass=" + analyzerClass.toString());
            analyzer = (Analyzer) analyzerClass.getConstructor(new Class[] {})
            .newInstance(new Object[] {});
            if (logger.isDebugEnabled())
                logger.debug("analyzer=" + analyzer.toString());
        } catch (ClassNotFoundException e) {
            throw new GenericSearchException(analyzerClassName
                    + ": class not found.\n", e);
        } catch (InstantiationException e) {
            throw new GenericSearchException(analyzerClassName
                    + ": instantiation error.\n", e);
        } catch (IllegalAccessException e) {
            throw new GenericSearchException(analyzerClassName
                    + ": instantiation error.\n", e);
        } catch (InvocationTargetException e) {
            throw new GenericSearchException(analyzerClassName
                    + ": instantiation error.\n", e);
        } catch (NoSuchMethodException e) {
            throw new GenericSearchException(analyzerClassName
                    + ": instantiation error.\n", e);
        }
        return analyzer;
    }
    
    public FSDirectory getDirectoryImplementation(String dirImplClassName, File file)
    throws GenericSearchException {
        FSDirectory directory = null;
        if (logger.isDebugEnabled())
            logger.debug("directoryImplementationClassName=" + dirImplClassName);
        try {
            Class dirImplClass = Class.forName(dirImplClassName);
            if (logger.isDebugEnabled())
                logger.debug("directoryImplementationClass=" + dirImplClass.toString());
            directory = (FSDirectory) dirImplClass.getConstructor(new Class[] {File.class})
            .newInstance(new Object[] {file});
            if (logger.isDebugEnabled())
                logger.debug("directory=" + directory.toString());
        } catch (ClassNotFoundException e) {
            throw new GenericSearchException(dirImplClassName
                    + ": class not found.\n", e);
        } catch (InstantiationException e) {
            throw new GenericSearchException(dirImplClassName
                    + ": instantiation error.\n", e);
        } catch (IllegalAccessException e) {
            throw new GenericSearchException(dirImplClassName
                    + ": instantiation error.\n", e);
        } catch (InvocationTargetException e) {
            throw new GenericSearchException(dirImplClassName
                    + ": instantiation error.\n", e);
        } catch (NoSuchMethodException e) {
            throw new GenericSearchException(dirImplClassName
                    + ": instantiation error.\n", e);
        }
        return directory;
    }
    
    public Analyzer getQueryAnalyzer(String indexName)
    throws GenericSearchException {
        Analyzer analyzer = getAnalyzer(config.getAnalyzer(indexName));
        PerFieldAnalyzerWrapper pfanalyzer = new PerFieldAnalyzerWrapper(analyzer);
    	StringTokenizer untokenizedFields = new StringTokenizer(config.getUntokenizedFields(indexName));
    	while (untokenizedFields.hasMoreElements()) {
    		pfanalyzer.addAnalyzer(untokenizedFields.nextToken(), new KeywordAnalyzer());
    	}
        if (logger.isDebugEnabled())
            logger.debug("getQueryAnalyzer indexName=" + indexName+ " untokenizedFields="+untokenizedFields);
        return pfanalyzer;
    }
    
    private long indexDirSpace(final File dir) {
    	long ids = 0;
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
        	File f = files[i];
            if (f.isDirectory()) {
            	ids += indexDirSpace(f);
            } else {
            	ids += f.length();
            }
        }
		return ids;
    }
    
}