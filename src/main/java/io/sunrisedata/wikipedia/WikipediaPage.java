package io.sunrisedata.wikipedia;

import com.amazonaws.util.StringInputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by evamonsen on 9/10/15.
 */
public class WikipediaPage {

  protected static final String XML_TAG_TITLE = "title";
  protected static final String XML_TAG_ID = "id";
  protected static final String XML_TAG_NAMESPACE = "ns";
  protected static final String XML_TAG_RESTRICTIONS = "restrictions";
  protected static final String XML_TAG_REDIRECT = "redirect";
  protected static final String XML_ATTRIBUTE_REDIRECT_TITLE = "title";

  private String redirectsTo;
  private String restrictions;
  private String namespace;
  private String title;
  private String pageId;


  public void readFromXml(String xml) throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(new StringInputStream(xml));

    NodeList n = doc.getChildNodes().item(0).getChildNodes();
    for(int i = 0; i < n.getLength(); i++) {
      Node node = n.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        Element e = (Element) node;
        switch (e.getTagName()) {
          case XML_TAG_TITLE:
            this.title = e.getTextContent();
            break;
          case XML_TAG_ID:
            this.pageId = e.getTextContent();
            break;
          case XML_TAG_NAMESPACE:
            this.namespace = e.getTextContent();
            break;
          case XML_TAG_RESTRICTIONS:
            this.restrictions = e.getTextContent();
            break;
          case XML_TAG_REDIRECT:
            this.redirectsTo = e.getAttribute(XML_ATTRIBUTE_REDIRECT_TITLE);
            break;
          default:
            break;
        }
      }
    }
  }

  public String getPageId() {
    return pageId;
  }

  public String getTitle() {
    return title;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getRestrictions() {

    return restrictions;
  }

  public String getRedirectsTo() {

    return redirectsTo;
  }


}
