package gobblin.writer;

import gobblin.configuration.State;
import gobblin.util.WriterUtils;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class AvroToParquetDataWriterBuilder extends
		DataWriterBuilder<Schema, GenericRecord> {
	private static final Logger LOG = LoggerFactory.getLogger(AvroToParquetDataWriterBuilder.class);
	
	@Override
	public DataWriter<GenericRecord> build() throws IOException {
		Preconditions.checkNotNull(this.destination);
		Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
		Preconditions.checkNotNull(this.schema);
		Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

		switch (this.destination.getType()) {
		case HDFS:
			State properties = this.destination.getProperties();

			String fileName = WriterUtils.getWriterFileName(properties,
					this.branches, this.branch, this.writerId,
					this.format.getExtension());

			try {
				return new AvroToParquetHdfsDataWriter(properties, fileName, this.schema,
						this.branches, this.branch);
			} catch (Exception e) {
				LOG.error("Failed to initialize parque writer.", e);
				throw new RuntimeException(e);
			}
		case KAFKA:
			return new AvroKafkaDataWriter();
		default:
			throw new RuntimeException("Unknown destination type: "
					+ this.destination.getType());
		}
	}

}
