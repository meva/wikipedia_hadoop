package io.sunrisedata.wikipedia_jar.test;

import io.sunrisedata.wikipedia.WikipediaPage;
import io.sunrisedata.wikipedia.WikipediaPageRevision;
import io.sunrisedata.wikipedia.WikipediaPageRevisionInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Created by evamonsen on 9/11/15.
 */
public class WikipediaPageRevisionInputReaderTest {

  @BeforeClass
  public static void classSetUp() {
    Logger rootLogger = Logger.getRootLogger();
    if (!rootLogger.getAllAppenders().hasMoreElements()) {
      rootLogger.setLevel(Level.DEBUG);
      rootLogger.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
    }
  }

  @Test
  public void canInitializeReader() throws IOException, InterruptedException {
    getReaderForFile(this.getClass().getClassLoader().getResource("dump1.xml").getFile(), 0, 1);
  }

  
  // nextKeyValueIsCorrectWhenPageStartTagInBlockAndRevisionStartTagOutsideOfBlock 2500, 200 // page start tag in block, revision start tag outside
  @Test
  public void nextKeyValueIsCorrectWhenPageStartTagInBlockAndRevisionStartTagOutsideOfBlock() throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = getReaderForFile(
        this.getClass().getClassLoader().getResource("dump2.xml").getFile(),
        2500, 200 // page start tag in block, revision start tag outside
    );
    checkPage1BothRevisions(reader);

    boolean anotherRev = reader.nextKeyValue();
    Assert.assertFalse("non-existent revision 3 is read", anotherRev);
  }

  // nextKeyValueIsCorrectWhenEntirePageWithinBlock 2600, 1000 // full page element within block
  @Test
  public void nextKeyValueIsCorrectWhenEntirePageWithinBlock() throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = getReaderForFile(
        this.getClass().getClassLoader().getResource("dump2.xml").getFile(),
        2600, 990 // full page element within block
    );
    checkPage1BothRevisions(reader);
    boolean anotherRev = reader.nextKeyValue();
    Assert.assertFalse("non-existent revision 3 is read", anotherRev);

  }

  // nextKeyValueIsCorrectWhenAllButPageEndWithinBlock 2600, 980 // block ends after </revision> and before </page>
  @Test
  public void nextKeyValueIsCorrectWhenAllButPageEndWithinBlock() throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = getReaderForFile(
        this.getClass().getClassLoader().getResource("dump2.xml").getFile(),
        2600, 980 // block ends after </revision> and before </page>
    );
    checkPage1BothRevisions(reader);
    checkNoMoreRevisions(reader);
  }

  @Test
  public void nextKeyValueIsCorrectWhenPageStartTagAndRevisionStartTagInBlock() throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = getReaderForFile(
        this.getClass().getClassLoader().getResource("dump2.xml").getFile(),
        2500, 500 // page start & revision start in block (end tags are not. should give 2 revisions)
    );

    checkPage1BothRevisions(reader);
    checkNoMoreRevisions(reader);
  }

  @Test
  public void nextKeyValueIsCorrectWhenPageStartTagOnlyPartlyInBlock() throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = getReaderForFile(
        this.getClass().getClassLoader().getResource("dump2.xml").getFile(),
        2610, 10 // start pos is in the middle of the <page> tag
    );
    boolean b = reader.nextKeyValue();

    Assert.assertFalse("page starts within the block", b);
  }


  @Test
  public void nextKeyValueIsCorrectWhenNoPageInBlock() throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = getReaderForFile(
        this.getClass().getClassLoader().getResource("dump2.xml").getFile(),
        0, 10
    );
    boolean b = reader.nextKeyValue();

    Assert.assertFalse("a key/value pair starts within the block", b);
  }

  @Test
  public void nextKeyValueIsCorrectWhenTwoPagesStartInBlock() throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = getReaderForFile(
        this.getClass().getClassLoader().getResource("dump2.xml").getFile(),
        0, 4900 // two <page> elements are in the block
    );
    checkPage1BothRevisions(reader);
    checkPage2AllRevisions(reader);
    checkNoMoreRevisions(reader);
  }

  private WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader getReaderForFile(String filepath, long start, long length) throws IOException, InterruptedException {
    WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader = new WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader();
    Configuration conf = new Configuration(true);

    Path p = new Path(filepath);

    JobID jobId = new JobID("local", 1000);
    TaskID taskId= new TaskID(jobId, TaskType.MAP, 100);
    TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 1);
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, taskAttemptId);

    FileSplit split = new FileSplit(p, start, length, null);
    reader.initialize(split, context);
    return reader;
  }

  private void checkPage1BothRevisions(WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader) throws IOException, InterruptedException {
    boolean firstRev = reader.nextKeyValue();
    Assert.assertTrue("revision 1 is read", firstRev);

    Text key = reader.getCurrentKey();
    Assert.assertEquals("10_233192", key.toString());

    WikipediaPageRevision value1 = reader.getCurrentValue();
    checkDump2Page1Revision1(value1);

    boolean secondRev = reader.nextKeyValue();
    Assert.assertTrue("revision 2 is read", secondRev);

    Text key2 = reader.getCurrentKey();
    Assert.assertEquals("10_862220", key2.toString());

    WikipediaPageRevision value2 = reader.getCurrentValue();
    Assert.assertSame("Same containing page object", value1.getContainingPage(), value2.getContainingPage());

    checkDump2Page1Revision2(value2);

  }

  private void checkPage2AllRevisions(WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader) throws IOException, InterruptedException {
    boolean firstRev = reader.nextKeyValue();
    Assert.assertTrue("revision 1 is read", firstRev);

    Text key1 = reader.getCurrentKey();
    Assert.assertEquals("12_18201", key1.toString());

    WikipediaPageRevision value1 = reader.getCurrentValue();
    checkDump2Page2Revision1(value1);

    boolean secondRev = reader.nextKeyValue();
    Assert.assertTrue("revision 2 is read", secondRev);

    WikipediaPageRevision value2 = reader.getCurrentValue();
    Assert.assertSame("Same containing page object", value1.getContainingPage(), value2.getContainingPage());
    Text key2 = reader.getCurrentKey();
    Assert.assertEquals("12_19746", key2.toString());

    checkDump2Page2Revision2(value2);

    boolean thirdRev = reader.nextKeyValue();
    Assert.assertTrue("revision 2 is read", thirdRev);

    Text key3 = reader.getCurrentKey();
    Assert.assertEquals("12_19749", key3.toString());

    WikipediaPageRevision value3 = reader.getCurrentValue();
    Assert.assertSame("Same containing page object", value1.getContainingPage(), value2.getContainingPage());

    checkDump2Page2Revision3(value3);

  }


  private void checkNoMoreRevisions(WikipediaPageRevisionInputFormat.WikipediaPageRevisionRecordReader reader)
      throws IOException, InterruptedException {
    boolean anotherRev = reader.nextKeyValue();
    Assert.assertFalse("non-existent revision 3 is read", anotherRev);

  }

  private void checkDump2Page1(WikipediaPage page1) {
    Assert.assertNotNull(page1);
    Assert.assertEquals("AccessibleComputing", page1.getTitle());
    Assert.assertEquals("10", page1.getPageId());
    Assert.assertEquals("0", page1.getNamespace());
    Assert.assertEquals("Computer accessibility", page1.getRedirectsTo());
    Assert.assertNull(page1.getRestrictions());

  }

  private void checkDump2Page2(WikipediaPage page2) {
    Assert.assertNotNull(page2);
    Assert.assertEquals("Anarchism", page2.getTitle());
    Assert.assertEquals("0", page2.getNamespace());
    Assert.assertEquals("12", page2.getPageId());
    Assert.assertNull(page2.getRedirectsTo());
    Assert.assertNull(page2.getRestrictions());

  }

  private void checkDump2Page1Revision1(WikipediaPageRevision value1) {
    checkDump2Page1(value1.getContainingPage());

    Assert.assertEquals("10", value1.getContainingPage().getPageId());
    Assert.assertEquals("233192", value1.getRevisionId());
    Assert.assertEquals("2001-01-21T02:12:21Z", value1.getTimestamp());
    Assert.assertEquals("99", value1.getContributorId());
    Assert.assertEquals("RoseParks", value1.getContributorUsername());
    Assert.assertEquals("*", value1.getComment());
    Assert.assertEquals("wikitext", value1.getModel());
    Assert.assertEquals("text/x-wiki", value1.getFormat());
    Assert.assertEquals("8kul9tlwjm9oxgvqzbwuegt9b2830vw", value1.getSha1());
    Assert.assertEquals("", value1.getRawContent());
    Assert.assertEquals(124, value1.getDeclaredContentLength());

  }

  private void checkDump2Page1Revision2(WikipediaPageRevision value2) {
    checkDump2Page1(value2.getContainingPage());
    Assert.assertEquals("862220", value2.getRevisionId());
    Assert.assertEquals("233192", value2.getParentRevisionId());
    Assert.assertEquals("2002-02-25T15:43:11Z", value2.getTimestamp());
    Assert.assertEquals("0", value2.getContributorId());
    Assert.assertEquals("Conversion script", value2.getContributorUsername());
    Assert.assertEquals("Automated conversion", value2.getComment());
    Assert.assertEquals("wikitext", value2.getModel());
    Assert.assertEquals("text/x-wiki", value2.getFormat());
    Assert.assertEquals("i8pwco22fwt12yp12x29wc065ded2bh", value2.getSha1());
    Assert.assertEquals("", value2.getRawContent());
    Assert.assertEquals(35, value2.getDeclaredContentLength());
  }

  private void checkDump2Page2Revision1(WikipediaPageRevision value1) {
    checkDump2Page2(value1.getContainingPage());

    Assert.assertEquals("18201", value1.getRevisionId());
    Assert.assertEquals("2002-02-25T15:00:22Z", value1.getTimestamp());
    Assert.assertEquals("0", value1.getContributorId());
    Assert.assertEquals("Conversion script", value1.getContributorUsername());
    Assert.assertEquals("Automated conversion", value1.getComment());
    Assert.assertEquals("wikitext", value1.getModel());
    Assert.assertEquals("text/x-wiki", value1.getFormat());
    Assert.assertEquals("07sqam7073877kptdznnip3viznphpy", value1.getSha1());
    Assert.assertEquals("", value1.getRawContent());
    Assert.assertEquals(9546, value1.getDeclaredContentLength());
  }

  private void checkDump2Page2Revision2(WikipediaPageRevision value2) {
    checkDump2Page2(value2.getContainingPage());

    Assert.assertEquals("19746", value2.getRevisionId());
    Assert.assertEquals("18201", value2.getParentRevisionId());
    Assert.assertEquals("2002-02-25T15:43:11Z", value2.getTimestamp());
    Assert.assertEquals("140.232.153.45", value2.getContributorIp());
    Assert.assertNull(value2.getContributorId());
    Assert.assertNull(value2.getContributorUsername());
    Assert.assertEquals("*", value2.getComment());
    Assert.assertEquals("wikitext", value2.getModel());
    Assert.assertEquals("text/x-wiki", value2.getFormat());
    Assert.assertEquals("px5ovjydixhpbysqmqiw7floem9ii5i", value2.getSha1());
    Assert.assertEquals("", value2.getRawContent());
    Assert.assertEquals(11279, value2.getDeclaredContentLength());
  }

  private void checkDump2Page2Revision3(WikipediaPageRevision value3) {
    checkDump2Page2(value3.getContainingPage());

    Assert.assertEquals("19749", value3.getRevisionId());
    Assert.assertEquals("19746", value3.getParentRevisionId());
    Assert.assertEquals("2002-02-27T17:34:09Z", value3.getTimestamp());
    Assert.assertEquals("24.188.31.147", value3.getContributorIp());
    Assert.assertNull(value3.getContributorId());
    Assert.assertNull(value3.getContributorUsername());
    Assert.assertEquals("*", value3.getComment());
    Assert.assertEquals("wikitext", value3.getModel());
    Assert.assertEquals("text/x-wiki", value3.getFormat());
    Assert.assertEquals("ir7scjzn4w7y93it75q41tamjrfd3nd", value3.getSha1());
    Assert.assertEquals("", value3.getRawContent());
    Assert.assertEquals(11394, value3.getDeclaredContentLength());
  }

}
