/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License, Version 1.0 only
 * (the "License").  You may not use this file except in compliance
 * with the License.
 *
 * You can obtain a copy of the license at license/ESCIDOC.LICENSE
 * or http://www.escidoc.de/license.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at license/ESCIDOC.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 * Copyright 2006-2008 Fachinformationszentrum Karlsruhe Gesellschaft
 * fuer wissenschaftlich-technische Information mbH and Max-Planck-
 * Gesellschaft zur Foerderung der Wissenschaft e.V.  
 * All rights reserved.  Use is subject to license terms.
 */
package dk.defxws.fgslucene;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.escidoc.sb.common.Constants;
import dk.defxws.fedoragsearch.server.Config;
import dk.defxws.fedoragsearch.server.errors.GenericSearchException;

/**
 * @author mih
 * 
 *         Singleton for caching IndexWriters (one for each index)
 * 
 */
public final class IndexWriterCache {

    private static IndexWriterCache instance = null;

    private static final Logger logger = 
    	LoggerFactory.getLogger(IndexWriterCache.class);
    
    /** Holds IndexWriter for each index. */
    private Map<String, IndexWriter> indexWriters = 
                        new HashMap<String, IndexWriter>();

    /**
     * private Constructor for Singleton.
     * 
     */
    private IndexWriterCache() {
    }

    /**
     * Only initialize Object once. Check for old objects in cache.
     * 
     * @return IndexWriterCache IndexWriterCache
     * 
     */
    public static synchronized IndexWriterCache getInstance() {
        if (instance == null) {
            instance = new IndexWriterCache();
        }
        return instance;
    }

    /**
     * delete document with given PID in index with given indexName.
     * 
     * @param indexName
     *            name of index to open.
     * @param config
     *            gsearch config-Object.
     * @param pid
     *            PID to update.
     * @param commit
     *            wether to commit indexWriter or not.
     * @throws GenericSearchException
     *             e
     */
	public void delete(
			final String indexName, 
			final Config config, 
			final String pid, 
			final boolean commit)
			throws GenericSearchException {
		try {
        	getIndexWriter(indexName, false, config).deleteDocuments(new Term("PID", pid));
		} catch (IOException e) {
			throw new GenericSearchException(
					"updateIndex deletePid error indexName="+indexName+" pid="+pid+"\n", e);
		} finally {
            if (commit) {
                closeIndexWriter(indexName);
            }
		}
	}

    /**
     * update document with given PID in index with given indexName.
     * 
     * @param indexName
     *            name of index to open.
     * @param config
     *            gsearch config-Object.
     * @param pid
     *            PID to update.
     * @param doc
     *            Update-Document.
     * @param commit
     *            wether to commit indexWriter or not.
     * @throws GenericSearchException
     *             e
     */
	public void update(
			final String indexName, 
			final Config config, 
			final String pid, 
			final Document doc, 
			final boolean commit)
			throws GenericSearchException {
		try {
        	getIndexWriter(indexName, false, config).updateDocument(new Term("PID", pid), doc);
		} catch (IOException e) {
			throw new GenericSearchException(e.getMessage());
		} finally {
            if (commit) {
                closeIndexWriter(indexName);
            }
		}
	}

    /**
     * optimize index for given indexName.
     * 
     * @param indexName
     *            name of index to open.
     * @param config
     *            gsearch config-Object.
     * @throws GenericSearchException
     *             e
     */
	public void optimize(final String indexName, final Config config)
			throws GenericSearchException {
		try {
			getIndexWriter(indexName, false, config).optimize();
		} catch (IOException e) {
			throw new GenericSearchException(
					"updateIndex optimize error indexName="+indexName+"\n", e);
		} finally {
            closeIndexWriter(indexName);
		}
	}

    /**
     * commit IndexWriter to persist to File.
     * 
     * @param indexName
     *            name of index to open.
     * @param config
     *            gsearch config-Object.
     * @throws GenericSearchException
     *             e
     */
	public void commit(final String indexName, final Config config)
			throws GenericSearchException {
		try {
			closeIndexWriter(indexName);
		} catch (IOException e) {
			throw new GenericSearchException(
					"commit error indexName="+indexName+"\n", e);
		}
	}

    /**
     * create empty index for given indexName.
     * 
     * @param indexName
     *            name of index to open.
     * @param config
     *            gsearch config-Object.
     * @throws GenericSearchException
     *             e
     */
	public void createEmpty(
			final String indexName, final Config config)
			throws GenericSearchException {
        closeIndexWriter(indexName);
        getIndexWriter(indexName, true, config);
        closeIndexWriter(indexName);
	}

    /**
     * get IndexWriter for given indexPath and write it into
     * cache.
     * 
     * @param indexName
     *            name of index to open.
     * @param config
     *            gsearch config-Object.
     * @throws GenericSearchException
     *             e
     */
	private synchronized IndexWriter getIndexWriter(
			final String indexName, final boolean create, final Config config) throws GenericSearchException {
		if (indexWriters.get(indexName) == null) {
	        IndexWriter iw = null;
            try {
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
                        Constants.LUCENE_VERSION,
                        getAnalyzer(config.getAnalyzer(indexName)));
                 if (create) {
                    indexWriterConfig.setOpenMode(OpenMode.CREATE);
                } else {
                    indexWriterConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
                }
                if (config.getMaxBufferedDocs(indexName) > 1) {
                    indexWriterConfig.setMaxBufferedDocs(config
                            .getMaxBufferedDocs(indexName));
                } 
                if (config.getRamBufferSize(indexName) 
                		!= IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB) {
                    indexWriterConfig.setRAMBufferSizeMB(config
                            .getRamBufferSize(indexName));
                }
				if (config.getMergeFactor(indexName) > 1 || config.getMaxMergeDocs(indexName) > 1 || config.getMaxMergeMb(indexName) > 1) {
					LogByteSizeMergePolicy logMergePolicy = new LogByteSizeMergePolicy();
					if (config.getMergeFactor(indexName) > 1) {
						logMergePolicy.setMergeFactor(config
								.getMergeFactor(indexName));
					}
					if (config.getMaxMergeDocs(indexName) > 1) {
						logMergePolicy.setMaxMergeDocs(config
								.getMaxMergeDocs(indexName));
					}
					if (config.getMaxMergeMb(indexName) > 1) {
						logMergePolicy.setMaxMergeMB(config
								.getMaxMergeMb(indexName));
					}
					indexWriterConfig.setMergePolicy(logMergePolicy);
				}
                if (config.getDefaultWriteLockTimeout(indexName) > 1) {
                    indexWriterConfig.setWriteLockTimeout(config
                            .getDefaultWriteLockTimeout(indexName));
                }
				iw = new IndexWriter(FSDirectory.open(new File(config
						.getIndexDir(indexName))), indexWriterConfig);
				if (config.getMaxChunkSize(indexName) > 1) {
					if (iw.getDirectory() instanceof MMapDirectory){
						((MMapDirectory)iw.getDirectory()).setMaxChunkSize(config
								.getMaxChunkSize(indexName));
					}
				}
            } catch (Exception e) {
            	iw = null;
                throw new GenericSearchException("IndexWriter new error, creating index indexName=" + indexName+ " :\n", e);
            }
	        indexWriters.put(indexName, iw);
	        return iw;
		}
		return indexWriters.get(indexName);
	}

    /**
     * close IndexWriter for given indexPath.
     * 
     * @param indexName
     *            name of index to open.
     * @throws GenericSearchException
     *             e
     */
	private synchronized void closeIndexWriter(
			final String indexName)
			throws GenericSearchException {
		try {
			if (indexWriters.get(indexName) != null) {
				indexWriters.get(indexName).close();
				indexWriters.put(indexName, null);
			}
		} catch (IOException e) {
			IndexWriter iw = indexWriters.get(indexName);
			iw = null;
			indexWriters.put(indexName, null);
			throw new GenericSearchException(e.getMessage());
		}
	}

    /**
     * commits changes in IndexWriter for given indexPath.
     * 
     * @param iw
     *            IndexWriter to commit.
     * @throws GenericSearchException
     *             e
     */
	private synchronized void commitIndexWriter(
			final String indexName, final Config config)
			throws GenericSearchException {
		try {
			getIndexWriter(indexName, false, config).commit();
		} catch (IOException e) {
			closeIndexWriter(indexName);
			throw new GenericSearchException(e.getMessage());
		}
	}

    /**
     * get Analyzer Object from ClassName.
     * 
     * @param analyzerClassName
     *            name of Analyzer-class.
     * @throws GenericSearchException
     *             e
     */
    private Analyzer getAnalyzer(String analyzerClassName)
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
        } catch (Exception e) {
            throw new GenericSearchException(analyzerClassName
                    + ": instantiation error.\n", e);
        }
        return analyzer;
    }
    
}
