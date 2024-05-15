package com.hobbitcakes.gcp.hl7v2.common;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;


public interface HL7V2toHL7V2Options extends DataflowPipelineOptions {

  @Description("The subscription ID for the source hl7V2Store subscription")
  @Default.String("projects/example-project-id/subscriptions/source-subscription")
  String getSourceSubscription();
  void setSourceSubscription(String sourceTopic);

  @Description("Full name of the destination hl7V2Store")
  @Default.String("projects/example-dest-project-id/locations/us-central1/datasets/destination-dataset/hl7V2Stores/destination-store")
  String getDestinationHL7V2Store();
  void setDestinationHL7V2Store(String destinationHL7V2Store);
}
