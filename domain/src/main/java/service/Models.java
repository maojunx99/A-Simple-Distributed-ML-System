package service;

import org.tensorflow.*;
import org.tensorflow.types.UInt8;
import utils.LogGenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/** Sample use of the TensorFlow Java API to label images using a pre-trained model. */
public class Models {
    static String modelDirResNet = Main.modelDirectory + "pretrained_resnet50";
    static String modelDirInception = Main.modelDirectory + "pretrained_inception_v3";
    private SavedModelBundle resnetBundle;
    private SavedModelBundle inceptionBundle;
    private final byte[] graphDefResNet;
    private final byte[] graphDefInception;
    private final List<String> labelsResNet;
    private final List<String> labelsInception;
    private Session resNetSession;
    private Session inceptionSession;
    private static int countResNet = 0;
    private static int countInception = 0;
    public static final HashMap<String, Integer> models = new HashMap<>();

    public final String modelPath = "../models.properties";
    public Models() throws IOException {
        graphDefResNet = readAllBytesOrExit(Paths.get(modelDirResNet, "saved_model.pb"));
        resnetBundle = SavedModelBundle.load(modelDirResNet, "serve");
        resNetSession = resnetBundle.session();
        labelsResNet = readAllLinesOrExit(Paths.get(modelDirResNet, "ImageNetLabels.txt"));
        graphDefInception = readAllBytesOrExit(Paths.get(modelDirInception, "saved_model.pb"));
        inceptionBundle = SavedModelBundle.load(modelDirInception, "serve");
        inceptionSession = inceptionBundle.session();
        labelsInception = readAllLinesOrExit(Paths.get(modelDirInception, "ImageNetLabels.txt"));
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream(modelPath));
        int i = 0;
        for (String modelName: properties.getProperty("models").split(",")) {
            models.put(modelName, i++);
        }
    }

    /* option = 0 -> resnet; option = 1 -> inception*/
    public List<String> Inference(String[] query, String option) {
        byte[] graphDef;
        List<String> labels;
        List<String> inferenceResult = new ArrayList<>();
        if(Objects.equals(option, "ResNet50")){
            graphDef = graphDefResNet;
            labels = labelsResNet;
        }else{
            graphDef = graphDefInception;
            labels = labelsInception;
        }
        List<Tensor<?>> images = new ArrayList<>();
        for(String i : query){
            byte[] imageBytes = readAllBytesOrExit(Paths.get(i));
            images.add(constructAndExecuteGraphToNormalizeImage(imageBytes, models.get(option)));
        }
        for(float[] labelProbabilities : executeInceptionGraph(graphDef, images, models.get(option), query)){
            int bestLabelIdx = maxIndex(labelProbabilities);
//                System.out.println(
//                        String.format("BEST MATCH: %s (%.2f%% likely)",
//                                labels.get(bestLabelIdx+1),
//                                labelProbabilities[bestLabelIdx] * 100f));
            if(Objects.equals(option, "ResNet50")){
                inferenceResult.add(String.format("BEST MATCH: %s (%.2f%% likely)",
                                labels.get(bestLabelIdx+1),
                                labelProbabilities[bestLabelIdx] * 100f));
            }else{
                inferenceResult.add(String.format("BEST MATCH: %s (%.2f%% likely)",
                        labels.get(bestLabelIdx),
                        (labelProbabilities[bestLabelIdx] * 10f >= 100) ? 99.99f : labelProbabilities[bestLabelIdx] * 10f));
            }
        }
        return inferenceResult;
    }

    private Tensor<Float> constructAndExecuteGraphToNormalizeImage(byte[] imageBytes, int option) {
        try (Graph g = new Graph()) {
            GraphBuilder b = new GraphBuilder(g);
            // Some constants specific to the pre-trained model at:
            // https://storage.googleapis.com/download.tensorflow.org/models/inception5h.zip
            //
            // - The model was trained with images scaled to 224x224 pixels.
            // - The colors, represented as R, G, B in 1-byte each were converted to
            //   float using (value - Mean)/Scale.
            int H,W;
            float mean,scale;
            if (option == 0){
                H = 224;
                W = 224;
            }else{
                H = 299;
                W = 299;
            }
            mean = 1f;
            scale = 255f;
            // Since the graph is being constructed once per execution here, we can use a constant for the
            // input image. If the graph were to be re-used for multiple input images, a placeholder would
            // have been more appropriate.
            final Output<String> input = b.constant("input", imageBytes);
            final Output<Float> output =
                    b.div(
                            b.sub(
                                    b.resizeBilinear(
                                            b.expandDims(
                                                    b.cast(b.decodeJpeg(input, 3), Float.class),
                                                    b.constant("make_batch", 0)),
                                            b.constant("size", new int[] {H, W})),
                                    b.constant("mean", mean)),
                            b.constant("scale", scale));
            try (Session s = new Session(g)) {
                // Generally, there may be multiple output tensors, all of them must be closed to prevent resource leaks.
                return s.runner().fetch(output.op().name()).run().get(0).expect(Float.class);
            }
        }
    }

    private List<float[]> executeInceptionGraph(byte[] graphDef, List<Tensor<?>> image, int option, String[] query) {
        try (Graph g = new Graph()) {
//            g.importGraphDef(graphDef);
            String feedName, fetchName;
            Session session;
            List<float[]> results = new ArrayList<>();
            if(option == 0){
                feedName = "serving_default_input_1";
                fetchName = "StatefulPartitionedCall";
                session = resNetSession;
                countResNet += image.size();
            }else{
                feedName = "serving_default_inputs";
                fetchName = "StatefulPartitionedCall";
                session = inceptionSession;
                countInception += image.size();
            }
                 // Generally, there may be multiple output tensors, all of them must be closed to prevent resource leaks.
            for(Tensor<?> i : image){
                Tensor<Float> result = session.runner().feed(feedName, i).fetch(fetchName).run().get(0).expect(Float.class);
                final long[] rshape = result.shape();
                if (result.numDimensions() != 2 || rshape[0] != 1) {
                    throw new RuntimeException(
                            String.format(
                                    "Expected model to produce a [1 N] shaped tensor where N is the number of labels, instead it produced one with shape %s",
                                    Arrays.toString(rshape)));
                }
                int nlabels = (int) rshape[1];
                results.add(result.copyTo(new float[1][nlabels])[0]);
            }
            if(option == 0 && countResNet > 10000){
                session.close();
                resnetBundle = SavedModelBundle.load(modelDirResNet, "serve");
                resNetSession = resnetBundle.session();
            }else if(option == 1 && countInception > 10000){
                session.close();
                inceptionBundle = SavedModelBundle.load(modelDirInception, "serve");
                inceptionSession = inceptionBundle.session();
            }
            return results;
        }
    }

    private static int maxIndex(float[] probabilities) {
        int best = 0;
        for (int i = 1; i < probabilities.length; ++i) {
            if (probabilities[i] > probabilities[best]) {
                best = i;
            }
        }
        return best;
    }

    private static byte[] readAllBytesOrExit(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println("Failed to read [" + path + "]: " + e.getMessage());
            System.exit(1);
        }
        return null;
    }

    private static List<String> readAllLinesOrExit(Path path) {
        try {
            return Files.readAllLines(path, Charset.forName("UTF-8"));
        } catch (IOException e) {
            System.err.println("Failed to read [" + path + "]: " + e.getMessage());
            System.exit(0);
        }
        return null;
    }

    // In the fullness of time, equivalents of the methods of this class should be auto-generated from
    // the OpDefs linked into libtensorflow_jni.so. That would match what is done in other languages
    // like Python, C++ and Go.
    static class GraphBuilder {
        GraphBuilder(Graph g) {
            this.g = g;
        }

        Output<Float> div(Output<Float> x, Output<Float> y) {
            return binaryOp("Div", x, y);
        }

        <T> Output<T> sub(Output<T> x, Output<T> y) {
            return binaryOp("Sub", x, y);
        }

        <T> Output<Float> resizeBilinear(Output<T> images, Output<Integer> size) {
            return binaryOp3("ResizeBilinear", images, size);
        }

        <T> Output<T> expandDims(Output<T> input, Output<Integer> dim) {
            return binaryOp3("ExpandDims", input, dim);
        }

        <T, U> Output<U> cast(Output<T> value, Class<U> type) {
            DataType dtype = DataType.fromClass(type);
            return g.opBuilder("Cast", "Cast")
                    .addInput(value)
                    .setAttr("DstT", dtype)
                    .build()
                    .<U>output(0);
        }

        Output<UInt8> decodeJpeg(Output<String> contents, long channels) {
            return g.opBuilder("DecodeJpeg", "DecodeJpeg")
                    .addInput(contents)
                    .setAttr("channels", channels)
                    .build()
                    .<UInt8>output(0);
        }

        <T> Output<T> constant(String name, Object value, Class<T> type) {
            try (Tensor<T> t = Tensor.<T>create(value, type)) {
                return g.opBuilder("Const", name)
                        .setAttr("dtype", DataType.fromClass(type))
                        .setAttr("value", t)
                        .build()
                        .<T>output(0);
            }
        }
        Output<String> constant(String name, byte[] value) {
            return this.constant(name, value, String.class);
        }

        Output<Integer> constant(String name, int value) {
            return this.constant(name, value, Integer.class);
        }

        Output<Integer> constant(String name, int[] value) {
            return this.constant(name, value, Integer.class);
        }

        Output<Float> constant(String name, float value) {
            return this.constant(name, value, Float.class);
        }

        private <T> Output<T> binaryOp(String type, Output<T> in1, Output<T> in2) {
            return g.opBuilder(type, type).addInput(in1).addInput(in2).build().<T>output(0);
        }

        private <T, U, V> Output<T> binaryOp3(String type, Output<U> in1, Output<V> in2) {
            return g.opBuilder(type, type).addInput(in1).addInput(in2).build().<T>output(0);
        }
        private Graph g;
    }
}
