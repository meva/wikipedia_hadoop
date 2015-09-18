
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


package io.sunrisedata.wikipedia;

import com.amazonaws.util.StringInputStream;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import org.apache.commons.lang.StringEscapeUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * A page revision from Wikipedia.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikipediaPageRevision {

  private static final String IDENTIFIER_REDIRECTION_UPPERCASE = "#REDIRECT";
  private static final String IDENTIFIER_REDIRECTION_LOWERCASE = "#redirect";
  private static final String IDENTIFIER_STUB_TEMPLATE = "stub}}";
  protected static final String XML_TAG_ID = "id";
  protected static final String XML_TAG_PARENTID = "parentid";
  protected static final String XML_TAG_TIMESTAMP = "timestamp";
  protected static final String XML_TAG_CONTRIBUTOR = "contributor";
  protected static final String XML_TAG_MINOR = "minor";
  protected static final String XML_TAG_COMMENT = "comment";
  protected static final String XML_TAG_TEXT = "text";
  protected static final String XML_TAG_SHA1 = "sha1";
  protected static final String XML_TAG_MODEL = "model";
  protected static final String XML_TAG_FORMAT = "format";
  protected static final String XML_TAG_CONTRIBUTOR_USERNAME = "username";
  protected static final String XML_TAG_CONTRIBUTOR_ID = "id";
  protected static final String XML_TAG_CONTRIBUTOR_IP = "ip";
  private static final String XML_ATTRIBUTE_TEXT_BYTES = "bytes";
  private static final String XML_ATTRIBUTE_TEXT_ID = "id";
  private static final String XML_ATTRIBUTE_DELETED = "deleted";
  private static final String XML_ATTRIBUTE_VALUE_DELETED = "deleted";
  private String parentRevisionId;

  public WikipediaPage getContainingPage() {
    return containingPage;
  }

  /**
   * Page containing this revision
   */
  protected WikipediaPage containingPage;
  /**
   * Identifier for the revision consisting of the page title followed by the revision id
   */
  protected String revisionId;
  /**
   * The raw XML of the revision.
   */
  protected boolean isRedirect; // Redirect marker appears in the article text
  protected boolean isStub; // Stub indicator appears in the article text
  protected String contentWikiMarkup;

  private WikiModel wikiModel;
  private PlainTextConverter textConverter;
  protected String contributorUsername;
  protected String contributorId;
  protected String contributorIp;
  protected String timestamp;
  /**
   * Length of markup content as declared in the XML of the revision, or -1 if not declared.
   */
  private int declaredContentLength = -1;
  private boolean isMetadata = false;
  private boolean isMinor = false;
  private String comment;
  private String sha1;
  private String model;
  private String format;

  /**
   * Creates an empty <code>WikipediaPage</code> object.
   */
  public WikipediaPageRevision(WikipediaPage containingPage) {
    wikiModel = new WikiModel("", "");
    textConverter = new PlainTextConverter();
    this.containingPage = containingPage;
  }

  /**
   * Returns the article title and revision number.
   */
  public String getRevisionId() {
    return revisionId;
  }

  // Explictly remove <ref>...</ref>, because there are screwy things like this:
  // <ref>[http://www.interieur.org/<!-- Bot generated title -->]</ref>
  // where "http://www.interieur.org/<!--" gets interpreted as the URL by
  // Bliki in conversion to text
  private static final Pattern REF = Pattern.compile("<ref>.*?</ref>");

  private static final Pattern LANG_LINKS = Pattern.compile("\\[\\[[a-z\\-]+:[^\\]]+\\]\\]");
  private static final Pattern DOUBLE_CURLY = Pattern.compile("\\{\\{.*?\\}\\}");

  private static final Pattern URL = Pattern.compile("http://[^ <]+"); // Note, don't capture
  // possible HTML tag

  private static final Pattern HTML_TAG = Pattern.compile("<[^!][^>]*>"); // Note, don't capture
  // comments
  private static final Pattern HTML_COMMENT = Pattern.compile("<!--.*?-->", Pattern.DOTALL);
  protected static final Pattern PATTERN_CONTRIBUTOR_ID = Pattern.compile("TODO");

  public String getDocid() {
    return null;
  }

  /**
   * Returns the contents of this revision (page title + text).
   */
  public String getRenderedContent() {
    String s = getRawContent();

    // Bliki doesn't seem to properly handle inter-language links, so remove manually.
    s = LANG_LINKS.matcher(s).replaceAll(" ");

    wikiModel.setUp();
    s = containingPage.getTitle() + "\n" + wikiModel.render(textConverter, s);
    wikiModel.tearDown();

    // The way the some entities are encoded, we have to unescape twice.
    s = StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(s));

    s = REF.matcher(s).replaceAll(" ");
    s = HTML_COMMENT.matcher(s).replaceAll(" ");

    // Sometimes, URL bumps up against comments e.g., <!-- http://foo.com/-->
    // Therefore, we want to remove the comment first; otherwise the URL pattern might eat up
    // the comment terminator.
    s = URL.matcher(s).replaceAll(" ");
    s = DOUBLE_CURLY.matcher(s).replaceAll(" ");
    s = HTML_TAG.matcher(s).replaceAll(" ");

    return s;
  }

  public String getDisplayContent() {
    wikiModel.setUp();
    String s = "<h1>" + containingPage.getTitle() + "</h1>\n" + wikiModel.render(getRawContent());
    wikiModel.tearDown();

    s = DOUBLE_CURLY.matcher(s).replaceAll(" ");

    return s;
  }

  /**
   * Returns the text of this revision.
   */
  public String getRawContent() {
    return contentWikiMarkup;
  }

  /**
   * Checks to see if the most recent revision is a redirect page. A <code>WikipediaPage</code> is either an
   * article, a disambiguation page, a redirect page, or an empty page.
   *
   * @return <code>true</code> if this page is a redirect page
   */
  public boolean isRedirect() {
    return isRedirect;
  }

  /**
   * Checks to see if this revision is an empty page. A <code>WikipediaPage</code> is either an article,
   * a disambiguation page, a redirect page, or an empty page.
   *
   * @return <code>true</code> if this page is an empty page
   */
  public boolean isEmpty() {
    return contentWikiMarkup == null || contentWikiMarkup == "";
  }

  /**
   * Checks to see if this revision is a stub. Return value is only meaningful if this page isn't a
   * disambiguation page, a redirect page, or an empty page.
   *
   * @return <code>true</code> if this article is a stub
   */
  public boolean isStub() {
    return isStub;
  }

//  /**
//   * Returns the inter-language link to a specific language (if any).
//   *
//   * @param lang language
//   * @return title of the article in the foreign language if link exists, <code>null</code>
//   *         otherwise
//   */
//  public String findInterlanguageLink(String lang) {
//    int start = contentWikiMarkup.indexOf("[[" + lang + ":");
//
//    if (start < 0)
//      return null;
//
//    int end = contentWikiMarkup.indexOf("]]", start);
//
//    if (end < 0)
//      return null;
//
//    // Some pages have malformed links. For example, "[[de:Frances Willard]"
//    // in enwiki-20081008-pages-articles.xml.bz2 has only one closing square
//    // bracket. Temporary solution is to ignore malformed links (instead of
//    // trying to hack around them).
//    String link = contentWikiMarkup.substring(start + 3 + lang.length(), end);
//
//    // If a newline is found, it probably means that the link is malformed
//    // (see above comment). Abort in this case.
//    if (link.indexOf("\n") != -1) {
//      return null;
//    }
//
//    if (link.length() == 0)
//      return null;
//
//    return link;
//  }

  public String getContributorUsername() {
    return contributorUsername;
  }

  public String getContributorId() {
    return contributorId;
  }

  public String getContributorIp() {
    return contributorIp;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public boolean isMetadata() {
    return isMetadata;
  }

  public boolean isMinor() {
    return isMinor;
  }

  public String getComment() {
    return comment;
  }

  public String getSha1() {
    return sha1;
  }

  public String getModel() {
    return model;
  }

  public String getFormat() {
    return format;
  }

  public long getDeclaredContentLength() {
    return declaredContentLength;
  }

  public void setDeclaredContentLength(int declaredContentLength) {
    this.declaredContentLength = declaredContentLength;
  }

  public String getParentRevisionId() {
    return parentRevisionId;
  }

  public static class Link {
    private String anchor;
    private String target;

    private Link(String anchor, String target) {
      this.anchor = anchor;
      this.target = target;
    }

    public String getAnchorText() {
      return anchor;
    }

    public String getTarget() {
      return target;
    }

    public String toString() {
      return String.format("[target: %s, anchor: %s]", target, anchor);
    }
  }

//  public List<Link> extractLinks() {
//    int start = 0;
//    List<Link> links = Lists.newArrayList();
//
//    while (true) {
//      start = contentWikiMarkup.indexOf("[[", start);
//
//      if (start < 0) {
//        break;
//      }
//
//      int end = contentWikiMarkup.indexOf("]]", start);
//
//      if (end < 0) {
//        break;
//      }
//
//      String text = contentWikiMarkup.substring(start + 2, end);
//      String anchor = null;
//
//      // skip empty links
//      if (text.length() == 0) {
//        start = end + 1;
//        continue;
//      }
//
//      // skip special links
//      if (text.indexOf(":") != -1) {
//        start = end + 1;
//        continue;
//      }
//
//      // if there is anchor text, get only article title
//      int a;
//      if ((a = text.indexOf("|")) != -1) {
//        anchor = text.substring(a + 1, text.length());
//        text = text.substring(0, a);
//      }
//
//      if ((a = text.indexOf("#")) != -1) {
//        text = text.substring(0, a);
//      }
//
//      // ignore article-internal links, e.g., [[#section|here]]
//      if (text.length() == 0) {
//        start = end + 1;
//        continue;
//      }
//
//      if (anchor == null) {
//        anchor = text;
//      }
//      links.add(new Link(anchor, text));
//
//      start = end + 1;
//    }
//
//    return links;
//  }
//
//  public List<String> extractLinkTargets() {
//    return Lists.transform(extractLinks(), new Function<Link, String>() {
//      @Override
//      @Nullable
//      public String apply(@Nullable Link link) {
//        return link.getTarget();
//      }
//    });
//  }

  public void readFromXml(String xml) throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(new StringInputStream(xml));

    // and now the fun part


    NodeList n = doc.getChildNodes().item(0).getChildNodes();
    for(int i = 0; i < n.getLength(); i++) {
      Node node = n.item(i);
      if(node.getNodeType() == Node.ELEMENT_NODE) {
        Element e = (Element)node;
        switch(e.getTagName()) {
          case XML_TAG_CONTRIBUTOR:
            NodeList contribNodes = e.getChildNodes();
            for(int j = 0; j < contribNodes.getLength();j++) {
              Node contribNode = contribNodes.item(j);
              if(contribNode.getNodeType() == Node.ELEMENT_NODE) {
                Element contribEl = (Element)contribNode;
                switch(contribEl.getTagName()) {
                  case XML_TAG_CONTRIBUTOR_ID:
                    this.contributorId = contribEl.getTextContent();
                    break;
                  case XML_TAG_CONTRIBUTOR_IP:
                    this.contributorIp = contribEl.getTextContent();
                    break;
                  case XML_TAG_CONTRIBUTOR_USERNAME:
                    this.contributorUsername = contribEl.getTextContent();
                    break;
                }
              }
            }
            break;

          case XML_TAG_TEXT:
            contentWikiMarkup = e.getTextContent();
            if (e.hasAttribute(XML_ATTRIBUTE_TEXT_BYTES)) {
              this.declaredContentLength = Integer.parseInt(e.getAttribute(XML_ATTRIBUTE_TEXT_BYTES));
              if(this.declaredContentLength > 0 && isEmpty()) {
                this.isMetadata = true;
              }
            }
            // determine if article is a disambiguation, redirection, and/or stub page.
            // the first characters of the text must be equal to IDENTIFIER_REDIRECTION_UPPERCASE or IDENTIFIER_REDIRECTION_LOWERCASE
            this.isRedirect = contentWikiMarkup.startsWith(IDENTIFIER_REDIRECTION_LOWERCASE) || contentWikiMarkup.startsWith(IDENTIFIER_REDIRECTION_UPPERCASE);

            // to be a stub, the article must contain the IDENTIFIER_STUB_WIKIPEDIA_NAMESPACE or IDENTIFIER_STUB_TEMPLATE
            this.isStub = contentWikiMarkup.contains(IDENTIFIER_STUB_TEMPLATE);

            break;

          case XML_TAG_ID:
            this.revisionId = e.getTextContent();
            break;

          case XML_TAG_TIMESTAMP:
            this.timestamp = e.getTextContent();
            break;

          case XML_TAG_MINOR:
            // presence of the empty <minor/> tag indicates it is a minor revision
            this.isMinor = true;
            break;

          case XML_TAG_COMMENT:
            this.comment = e.getTextContent();
            break;

          case XML_TAG_SHA1:
            this.sha1 = e.getTextContent();
            break;

          case XML_TAG_MODEL:
            this.model = e.getTextContent();
            break;

          case XML_TAG_FORMAT:
            this.format = e.getTextContent();
            break;

          case XML_TAG_PARENTID:
            this.parentRevisionId = e.getTextContent();
            break;
        }
      }
    }

  }

}

