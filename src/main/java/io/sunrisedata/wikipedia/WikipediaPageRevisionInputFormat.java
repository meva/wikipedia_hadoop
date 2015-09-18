package io.sunrisedata.wikipedia;

/**
 * Created by evamonsen on 9/10/15.
 */
/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Hadoop {@code InputFormat} for processing Wikipedia page REVISIONS from the XML dumps.
 *
 * A bit of explanation. Hadoop provides randomly sized chunks of files which may not
 * entirely contain wikipedia &lt;page&gt; elements.
 *
 * We are allowed to read from the start of the split, and may read beyond the end of
 * the split.
 *
 * This class will read all of the pages that *start* within the split, even if they
 * end outside of the split.
 *
 * The revision key is the file path + start position of the revision, which should
 * uniquely identify the revision within a set of multiple input files.
 *
 * The value is the raw xml of the revision, including the &lt;revision&gt; start and end tags.
 *
 * @author Eva Monsen
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikipediaPageRevisionInputFormat extends FileInputFormat<Text, WikipediaPageRevision> {
  @Override
  public RecordReader<Text, WikipediaPageRevision> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new WikipediaPageRevisionRecordReader();
  }

  public static class WikipediaPageRevisionRecordReader extends RecordReader<Text, WikipediaPageRevision> {
    private static final Logger LOG = Logger.getLogger(WikipediaPageRevisionRecordReader.class);

    private WikipediaPageRevision revision;
    private WikipediaPage page;
    private String language;
    private byte[] revisionStartTag;
    private byte[] revisionEndTag;

    private byte[] pageStartTagBytes;
    private byte[] pageEndTag;
    byte[][] revisionSearchTags ;
    private long start;
    private long end;
    private long pos;
    private DataInputStream fsin = null;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private long recordStartPos;

    // keep track of state
    private boolean inPage = false;

    private final Text key = new Text();
    private WikipediaPageRevision value;
    private long pageStartPos;
    private Path file;
    private long revisionStartPos;
    private static final String PAGE_START_TAG = "<page>";
    private static final String REVISION_START_TAG = "<revision>";
    /**
     * Called once at initialization.
     *
     * @param input the split that defines the range of records to readFromXml
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit input, TaskAttemptContext context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      this.pageStartTagBytes = PAGE_START_TAG.getBytes("utf-8");
      this.pageEndTag = "</page>".getBytes("utf-8");
      this.revisionStartTag = REVISION_START_TAG.getBytes("utf-8");
      this.revisionEndTag = "</revision>".getBytes("utf-8");

      byte[][] b = {revisionStartTag, pageEndTag};
      revisionSearchTags = b;

      FileSplit split = (FileSplit) input;
      start = split.getStart();
      file = split.getPath();

      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
      CompressionCodec codec = compressionCodecs.getCodec(file);

      FileSystem fs = file.getFileSystem(conf);

      if (codec != null) {
        LOG.info("Reading compressed file " + file + "...");
        fsin = new DataInputStream(codec.createInputStream(fs.open(file)));

        end = Long.MAX_VALUE;
      } else {
        LOG.info("Reading uncompressed file " + file + "...");
        FSDataInputStream fileIn = fs.open(file);

        fileIn.seek(start);
        fsin = fileIn;

        end = start + split.getLength();
      }

      recordStartPos = start;

      // Because input streams of gzipped files are not seekable, we need to keep track of bytes
      // consumed ourselves.
      pos = start;
    }

    /**
     * Read the next key, value pair, provided the page
     *
     * @return {@code true} if a key/value pair was readFromXml
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      while(true) {
        // find page start if we're not in one
        if (!inPage) {
          LOG.debug("Searchign for start page");
          buffer.reset();
          if (readUntilMatch(pageStartTagBytes, false)) {
            pageStartPos = pos - pageStartTagBytes.length;
            inPage = true;
            LOG.debug("Page start is at "+pageStartPos);
          } else {
            LOG.debug("No page start tag found within block");
            return false;
          }
        }

        // look for either a revision start tag, or a page end tag
        switch (readUntilMatch(revisionSearchTags, true)) {
          case 0: // revision
            if(page == null) {
              LOG.debug("reading page");
              page = new WikipediaPage();
              try {
                String xml  = PAGE_START_TAG+new String(buffer.getData(), 0, buffer.getLength()-revisionStartTag.length, "utf-8")+"</page>";
                LOG.debug("Page Xml = "+xml);
                page.readFromXml(xml);
              } catch (ParserConfigurationException e) {
                LOG.error("Error reading WikipediaPage", e);
              } catch (SAXException e) {
                LOG.error("Error reading WikipediaPage", e);
              }
            }
            // find the whole xml of the revision
            revisionStartPos = pos - revisionStartTag.length;
            buffer.reset();
            buffer.write(revisionStartTag);
            if (readUntilMatch(revisionEndTag, true)) {
              //key.set(file + Long.toString(recordStartPos));
              value = new WikipediaPageRevision(page);
              try {
                String xml = new String(buffer.getData(), 0, buffer.getLength(), "utf-8");
                LOG.debug("revision xml = " + xml);
                value.readFromXml(xml);
                key.set(value.getContainingPage().getPageId() + "_" + value.getRevisionId());
              } catch (ParserConfigurationException e) {
                LOG.error("Error reading WikipediaPageRevision in page " + value.getContainingPage().getPageId(), e);
              } catch (SAXException e) {
                LOG.error("Error reading WikipediaPageRevision in page " + value.getContainingPage().getPageId(), e);
              }
              return true;
            }
            // didn't find revision end. that's weird. log it and run away
            LOG.error("no end tag for revision starting at position " + revisionStartPos + " in file " + file);
            return false;
          case 1: // end page
            // no more revisions for this page so go find start of next page.
            // note: this is the only case where we go through the while loop again
            inPage = false;
            page = null;
            break;
          default:
            // didn't find revision start OR page end. that's weird. log it and run away
            LOG.error("no end tag for page starting at position " + pageStartPos + " in file " + file + "");
            return false;
        }
      }
    }

    /**
     * Returns the current key.
     *
     * @return the current key or {@code null} if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    /**
     * Returns the current value.
     *
     * @return current value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public WikipediaPageRevision getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    /**
     * Closes the record reader.
     */
    @Override
    public void close() throws IOException {
      fsin.close();
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data readFromXml
     * @throws IOException
     */
    @Override
    public float getProgress() throws IOException {
      return ((float) (pos - start)) / ((float) (end - start));
    }

    /**
     *
     * @param match
     * @param saveToBuffer
     * @return
     * @throws IOException
     */
    private boolean readUntilMatch(byte[] match, boolean saveToBuffer)
        throws IOException {
      byte[][] matches = {match};
      return (readUntilMatch(matches, saveToBuffer) == 0);
    }

    /**
     *
     * @param matches
     * @param saveToBuffer whether to save bytes readFromXml to a buffer for later use.
     * @return index of the match, otherwise -1
     * @throws IOException
     */
    private int readUntilMatch(byte[][] matches, boolean saveToBuffer)
        throws IOException {
      int[] i = new int[matches.length]; // should be initialized to all zeroes
      while (true) {
        int b = fsin.read();
        // increment position (bytes consumed)
        pos++;

        // end of file:
        if (b == -1)
          return -1;
        // save to buffer:
        if (saveToBuffer)
          buffer.write(b);

        // check if we're matching:
        for(int m = 0; m < matches.length; m++) {
          byte[] match = matches[m];

          if (b == match[i[m]]) {
            i[m]++;
            if (i[m] >= match.length) {
              LOG.debug("Match "+m+" found at pos " + pos);
              return m;
            }
          } else {
            i[m] = 0;
          }
          // see if we've passed the stop point:
          if (!saveToBuffer && Arrays.stream(i).sum() == 0 && pos >= end) {
            LOG.debug("readUntilMatch returning -1");
            return -1;
          }
        }
      }
    }
  }

}

