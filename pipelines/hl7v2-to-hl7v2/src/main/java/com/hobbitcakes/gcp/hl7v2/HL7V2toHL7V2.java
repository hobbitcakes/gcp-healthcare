package com.hobbitcakes.gcp.hl7v2;

import com.google.common.base.Strings;
import com.hobbitcakes.gcp.hl7v2.common.HL7V2toHL7V2Options;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.healthcare.HL7v2IO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class HL7V2toHL7V2 {

  static void runHL7V2Pipeline(HL7V2toHL7V2Options options) {
    boolean hasSourceSubscription = !Strings.isNullOrEmpty(options.getSourceSubscription());
    boolean hasDestinationStore = !Strings.isNullOrEmpty(options.getDestinationHL7V2Store());

    if (!hasSourceSubscription && !hasDestinationStore) {
      throw new IllegalArgumentException(
          "Must specify both a source subscription and a destination HL7V2 store");
    }

    Pipeline p = Pipeline.create(options);
    HL7v2IO.Read.Result v2Messages = p.apply("Read from Subscription",
            PubsubIO.readStrings().fromSubscription(options.getSourceSubscription()))
        .apply("Read from HL7V2Store", HL7v2IO.getAll());
    // TODO: Handle errors in the result
    v2Messages.getMessages().apply("Write to HL7V2Store", HL7v2IO.ingestMessages(options.getDestinationHL7V2Store()));

    p.run();
  }

  public static void main(String[] args) {
    HL7V2toHL7V2Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(HL7V2toHL7V2Options.class);
    runHL7V2Pipeline(options);
  }

}
