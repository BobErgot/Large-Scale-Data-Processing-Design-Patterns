package mr.design.patterns.organization.hierarchical;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class QuestionAnswerBuildingDriver extends Configured implements Tool {

  public static class PostCommentMapper extends Mapper<Object, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();
    private final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      // Parse the post/comment XML hierarchy into an Element
      Element post = null;
      try {
        post = getXmlElementFromString(value.toString(), dbf);
      } catch (ParserConfigurationException | SAXException e) {
        Log.error("Error getting XML element from value");
      }

      if (post == null) return;

      int postType = Integer.parseInt(post.getAttribute("PostTypeId"));

      // If postType is 1, it is a question
      if (postType == 1) {
        outKey.set(post.getAttribute("Id"));
        outValue.set("Q" + value);
      } else {
        // Else, it is an answer
        outKey.set(post.getAttribute("ParentId"));
        outValue.set("A" + value);
      }

      context.write(outKey, outValue);
    }
  }

  public static class QuestionAnswerReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final ArrayList<String> answers = new ArrayList<String>();
    private final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    private String question = null;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      // Reset variables
      question = null;
      answers.clear();

      // For each input value
      for (Text t : values) {
        // If this is the post record, store it, minus the flag
        if (t.charAt(0) == 'Q') {
          question = t.toString().substring(1).trim();
        } else {
          // Else, it is a comment record. Add it to the list, minus the flag
          answers.add(t.toString().substring(1).trim());
        }
      }

      // If post is not null
      if (question != null) {
        // nest the comments underneath the post element
        String postWithCommentChildren = null;
        try {
          postWithCommentChildren = nestElements(question, answers, dbf);
        } catch (ParserConfigurationException | TransformerException | SAXException e) {
          Log.error("Error in nesting questions and answers");
          return;
        }
        // write out the XML
        context.write(new Text(postWithCommentChildren), NullWritable.get());
      }
    }
  }

    private static String nestElements(String post, List<String> comments, DocumentBuilderFactory dbf) throws ParserConfigurationException, TransformerException, IOException, SAXException {
      // Create the new document to build the XML
      DocumentBuilder bldr = dbf.newDocumentBuilder();
      Document doc = bldr.newDocument();

      // Copy parent node to document
      Element postEl = getXmlElementFromString(post, dbf);
      Element toAddPostEl = doc.createElement("question");

      // Copy the attributes of the original post element to the new one
      copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

      // For each comment, copy it to the "post" node
      for (String commentXml : comments) {
        Element commentEl = getXmlElementFromString(commentXml, dbf);
        Element toAddCommentEl = doc.createElement("answers");
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

    private static Element getXmlElementFromString(String xml, DocumentBuilderFactory dbf) throws ParserConfigurationException, IOException, SAXException {
      // Create a new document builder
      DocumentBuilder bldr = dbf.newDocumentBuilder();
      return bldr.parse(new InputSource(new StringReader(xml))).getDocumentElement();
    }

    private static void copyAttributesToElement(NamedNodeMap attributes, Element element) {
      // For each attribute, copy it to the element
      for (int i = 0; i < attributes.getLength(); ++i) {
        Attr toCopy = (Attr) attributes.item(i);
        element.setAttribute(toCopy.getName(), toCopy.getValue());
      }
    }

    private static String transformDocumentToString(Document doc) throws TransformerException {
      TransformerFactory tf = TransformerFactory.newInstance();
      Transformer transformer = tf.newTransformer();
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      StringWriter writer = new StringWriter();
      transformer.transform(new DOMSource(doc), new StreamResult(writer));
      // Replace all new line characters with an empty string to have one record per line.
      return writer.getBuffer().toString().replaceAll("\n|\r", "");
    }

    @Override
    public int run(String[] args) throws Exception {
      Configuration conf = getConf();
      GenericOptionsParser parser = new GenericOptionsParser(conf, args);
      String[] otherArgs = parser.getRemainingArgs();
      if (otherArgs.length != 2) {
        System.err.println("Usage: QuestionAnswerBuildingDriver <in> <out>");
        ToolRunner.printGenericCommandUsage(System.err);
        System.exit(2);
      }

      Job job = new Job(conf, "QuestionAnswerBuildingDriver");
      job.setJarByClass(QuestionAnswerBuildingDriver.class);
      job.setMapperClass(PostCommentMapper.class);
      job.setReducerClass(QuestionAnswerReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

      return job.waitForCompletion(true) ? 0 : 2;
    }

    public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new QuestionAnswerBuildingDriver(), args);
      System.exit(res);
    }
  }