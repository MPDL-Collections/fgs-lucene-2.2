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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.store.FSDirectory;
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
	public synchronized IndexWriter getIndexWriter(
			final String indexName, final boolean create, final Config config) throws GenericSearchException {
		if (indexWriters.get(indexName) == null) {
			IndexWriter iw = null;
			try {
				// MIH set maxFieldLength to Integer.MAX_VALUE
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
				if (config.getMergeFactor(indexName) > 1) {
					LogByteSizeMergePolicy logMergePolicy = new LogByteSizeMergePolicy();
					logMergePolicy.setMergeFactor(config
							.getMergeFactor(indexName));
					indexWriterConfig.setMergePolicy(logMergePolicy);
				}
				if (config.getDefaultWriteLockTimeout(indexName) > 1) {
					indexWriterConfig.setWriteLockTimeout(config
							.getDefaultWriteLockTimeout(indexName));
				}
				iw = new IndexWriter(FSDirectory.open(new File(config
						.getIndexDir(indexName))), indexWriterConfig);
			} catch (Exception e) {
				throw new GenericSearchException(
						"IndexWriter new error indexName=" + indexName
								+ " :\n", e);
			}
	        indexWriters.put(indexName, iw);
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
	public synchronized void closeIndexWriter(
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
     * get IndexReader for given indexPath.
     * 
     * @param indexName
     *            name of index to open.
     * @param config
     *            gsearch config-Object.
     * @throws GenericSearchException
     *             e
     */
	public synchronized IndexReader getIndexReader(
			final String indexName, final Config config)
			throws GenericSearchException {
		try {
			return IndexReader.open(getIndexWriter(indexName, false, config), false);
		} catch (IOException e) {
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
