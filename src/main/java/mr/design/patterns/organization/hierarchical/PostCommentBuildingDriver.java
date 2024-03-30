package mr.design.patterns.organization.hierarchical;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jline.utils.Log;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import mr.design.patterns.utils.MRDPUtils;

public class PostCommentBuildingDriver extends Configured implements Tool {

  public static abstract class HierarchyMapper extends Mapper<Object, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outValue = new Text();

    private final String fieldName;
    private final String valuePrefix;

    protected HierarchyMapper(String fieldName, String valuePrefix) {
      this.fieldName = fieldName;
      this.valuePrefix = valuePrefix;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

      String parsedId = parsed.get(fieldName);

      if (parsedId == null) {
        return;
      }

      // The foreign join key is the post ID
      outKey.set(parsedId);

      // Flag this record for the reducer and then output
      outValue.set(valuePrefix + value);
      context.write(outKey, outValue);
    }
  }

  public static class PostMapper extends HierarchyMapper {
    public PostMapper() {
      super("Id", "P");
    }
  }

  public static class CommentMapper extends HierarchyMapper {
    public CommentMapper() {
      super("PostId", "C");
    }
  }

  public static class PostCommentHierarchyReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final ArrayList<String> comments = new ArrayList<String>();
    private final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    private String post = null;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Reset variables
      post = null;
      comments.clear();

      // For each input value
      for (Text t : values) {
        // If this is the post record, store it, minus the flag
        if (t.charAt(0) == 'P') {
          post = t.toString().substring(1).trim();
        } else {
          // Else, it is a comment record. Add it to the list, minus the flag
          comments.add(t.toString().substring(1).trim());
        }
      }

      // If there are no comments, the comments list will simply be empty.
      // If post is not null, combine post with its comments.

      if (post != null) {
        // nest the comments underneath the post element
        String postWithCommentChildren = null;
        try {
          postWithCommentChildren = nestElements(post, comments);
        } catch (ParserConfigurationException| TransformerException| SAXException e) {
          Log.error("XML formation error");
          return;
        }

        // write out the XML
        context.write(new Text(postWithCommentChildren), NullWritable.get());
      }
    }

    private String nestElements(String post, List<String> comments) throws ParserConfigurationException, TransformerException, IOException, SAXException {
      // Create the new document to build the XML
      DocumentBuilder bldr = dbf.newDocumentBuilder();
      Document doc = bldr.newDocument();

      // Copy parent node to document
      Element postEl = getXmlElementFromString(post);
      Element toAddPostEl = doc.createElement("post");

      // Copy the attributes of the original post element to the new one
      copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

      // For each comment, copy it to the "post" node
      for (String commentXml : comments) {
        Element commentEl = getXmlElementFromString(commentXml);
        Element toAddCommentEl = doc.createElement("comments");
        // Copy the attributes of the original comment element to the new one
        copyAttributesToElement(commentEl.getAttributes(), toAddCommentEl);
        // Add the copied comment to the post element
        toAddPostEl.appendChild(toAddCommentEl);
      }

      // Add the post element to the document
      doc.appendChild(toAddPostEl);
      // Transform the document into a String of XML and return
      return transformDocumentToString(doc);
    }

    private Element getXmlElementFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
      // Create a new document builder
      DocumentBuilder bldr = dbf.newDocumentBuilder();
      return bldr.parse(new InputSource(new StringReader(xml))).getDocumentElement();
    }

    private void copyAttributesToElement(NamedNodeMap attributes, Element element) {
      // For each attribute, copy it to the element
      for (int i = 0; i < attributes.getLength(); ++i) {
        Attr toCopy = (Attr) attributes.item(i);
        element.setAttribute(toCopy.getName(), toCopy.getValue());
      }
    }

    private String transformDocumentToString(Document doc) throws TransformerException {
      TransformerFactory tf = TransformerFactory.newInstance();
      Transformer transformer = tf.newTransformer();transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      StringWriter writer = new StringWriter();
      transformer.transform(new DOMSource(doc), new StreamResult(writer));
      // Replace all new line characters with an empty string to have one record per line.
      return writer.getBuffer().toString().replaceAll("\n|\r", "");
    }
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: PostCommentBuildingDriver <Post_in> <out> <Comment_in>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    Job job = new Job(conf, "PostCommentHierarchy");
    job.setJarByClass(PostCommentBuildingDriver.class);

    MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, PostMapper.class);
    MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, CommentMapper.class);

    job.setReducerClass(PostCommentHierarchyReducer.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 2;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PostCommentBuildingDriver(), args);
    System.exit(res);
  }
}