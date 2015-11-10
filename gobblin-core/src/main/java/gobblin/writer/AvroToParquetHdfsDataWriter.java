package gobblin.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.FinalState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.JobConfigurationUtils;
import gobblin.util.ProxiedFileSystemWrapper;
import gobblin.util.WriterUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class AvroToParquetHdfsDataWriter implements DataWriter<GenericRecord>,
		FinalState {

	private static final Logger LOG = LoggerFactory
			.getLogger(AvroToParquetHdfsDataWriter.class);

	private final Schema schema;
	private final ParquetWriter<GenericRecord> writer;
	protected final State properties;
	protected final FileSystem fs;
	protected final Path stagingFile;
	protected final Path outputFile;
	protected final String outputFilePropName;
	protected final int bufferSize;
	protected final short replicationFactor;
	protected final long blockSize;
	protected final FsPermission filePermission;
	protected final FsPermission dirPermission;
	protected final Optional<String> group;

	// Number of records successfully written
	protected final AtomicLong count = new AtomicLong(0);

	public AvroToParquetHdfsDataWriter(State properties, String fileName,
			Schema schema, int numBranches, int branchId) throws Exception {
		this.properties = properties;
		this.schema = schema;
		Configuration conf = new Configuration();
		// Add all job configuration properties so they are picked up by Hadoop
		JobConfigurationUtils.putStateIntoConfiguration(properties, conf);

		String uri = properties.getProp(ForkOperatorUtils
				.getPropertyNameForBranch(
						ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches,
						branchId), ConfigurationKeys.LOCAL_FS_URI);

		if (properties.getPropAsBoolean(
				ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
				ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
			// Initialize file system as a proxy user.
			try {
				this.fs = new ProxiedFileSystemWrapper()
						.getProxiedFileSystem(
								properties,
								ProxiedFileSystemWrapper.AuthType.TOKEN,
								properties
										.getProp(ConfigurationKeys.FS_PROXY_AS_USER_TOKEN_FILE),
								uri);
			} catch (InterruptedException e) {
				throw new IOException(e);
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		} else {
			// Initialize file system as the current user.
			this.fs = FileSystem.get(URI.create(uri), conf);
		}

		// Initialize staging/output directory
		this.stagingFile = new Path(WriterUtils.getWriterStagingDir(properties,
				numBranches, branchId), fileName);
		this.outputFile = new Path(WriterUtils.getWriterOutputDir(properties,
				numBranches, branchId), fileName);
		this.outputFilePropName = ForkOperatorUtils.getPropertyNameForBranch(
				ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, numBranches,
				branchId);
		this.properties.setProp(this.outputFilePropName,
				this.outputFile.toString());

		// Deleting the staging file if it already exists, which can happen if
		// the
		// task failed and the staging file didn't get cleaned up for some
		// reason.
		// Deleting the staging file prevents the task retry from being blocked.
		if (this.fs.exists(this.stagingFile)) {
			LOG.warn(String.format(
					"Task staging file %s already exists, deleting it",
					this.stagingFile));
			HadoopUtils.deletePath(this.fs, this.stagingFile, false);
		}

		this.bufferSize = Integer.parseInt(properties.getProp(ForkOperatorUtils
				.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUFFER_SIZE,
						numBranches, branchId),
				ConfigurationKeys.DEFAULT_BUFFER_SIZE));

		this.replicationFactor = properties.getPropAsShort(ForkOperatorUtils
				.getPropertyNameForBranch(
						ConfigurationKeys.WRITER_FILE_REPLICATION_FACTOR,
						numBranches, branchId), this.fs
				.getDefaultReplication(this.outputFile));

		this.blockSize = properties
				.getPropAsLong(ForkOperatorUtils.getPropertyNameForBranch(
						ConfigurationKeys.WRITER_FILE_BLOCK_SIZE, numBranches,
						branchId), this.fs.getDefaultBlockSize(this.outputFile));

		this.filePermission = HadoopUtils.deserializeWriterFilePermissions(
				properties, numBranches, branchId);

		this.dirPermission = HadoopUtils.deserializeWriterDirPermissions(
				properties, numBranches, branchId);

		Optional<String> compresionCodecName = Optional.fromNullable(properties
				.getProp(ConfigurationKeys.WRITER_CODEC_TYPE));
		if (compresionCodecName.isPresent()) {
			conf.set(ParquetOutputFormat.COMPRESSION, compresionCodecName.get());
		}
		CompressionCodecName codec = CodecConfig
				.getParquetCompressionCodec(conf);

		this.writer = AvroParquetWriter
				.<GenericRecord> builder(this.stagingFile)
				.withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
				.withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
				.withCompressionCodec(codec).withSchema(this.schema)
				.withConf(conf).build();
		
		this.group = Optional.fromNullable(properties.getProp(ForkOperatorUtils
				.getPropertyNameForBranch(ConfigurationKeys.WRITER_GROUP_NAME,
						numBranches, branchId)));

		if (this.group.isPresent()) {
			HadoopUtils.setGroup(this.fs, this.stagingFile, this.group.get());
		} else {
			LOG.warn("No group found for " + this.stagingFile);
		}

		// Create the parent directory of the output file if it does not exist
		WriterUtils.mkdirsWithRecursivePermission(this.fs,
				this.outputFile.getParent(), this.dirPermission);

	}

	public FileSystem getFileSystem() {
		return this.fs;
	}

	@Override
	public void write(GenericRecord record) throws IOException {
		Preconditions.checkNotNull(record);
		this.writer.write(record);
		// Only increment when write is successful
		this.count.incrementAndGet();
	}

	@Override
	public long recordsWritten() {
		return this.count.get();
	}

	@Override
	public long bytesWritten() throws IOException {
		if (!this.fs.exists(this.outputFile)) {
			return 0;
		}

		return this.fs.getFileStatus(this.outputFile).getLen();
	}

	@Override
	public void close() throws IOException {
		this.writer.close();
	}

	@Override
	public State getFinalState() {
		State state = new State();

		state.setProp("RecordsWritten", recordsWritten());
		try {
			state.setProp("BytesWritten", bytesWritten());
		} catch (Exception exception) {
			// If Writer fails to return bytesWritten, it might not be
			// implemented, or implemented incorrectly.
			// Omit property instead of failing.
		}

		return state;
	}

	@Override
	public void commit() throws IOException {
		if (!this.fs.exists(this.stagingFile)) {
			throw new IOException(String.format("File %s does not exist",
					this.stagingFile));
		}

		// Double check permission of staging file
		if (!this.fs.getFileStatus(this.stagingFile).getPermission()
				.equals(this.filePermission)) {
			this.fs.setPermission(this.stagingFile, this.filePermission);
		}

		LOG.info(String.format("Moving data from %s to %s", this.stagingFile,
				this.outputFile));
		// For the same reason as deleting the staging file if it already
		// exists, deleting
		// the output file if it already exists prevents task retry from being
		// blocked.
		if (this.fs.exists(this.outputFile)) {
			LOG.warn(String.format("Task output file %s already exists",
					this.outputFile));
			HadoopUtils.deletePath(this.fs, this.outputFile, false);
		}

		HadoopUtils.renamePath(this.fs, this.stagingFile, this.outputFile);
	}

	@Override
	public void cleanup() throws IOException {
		// Delete the staging file
		if (this.fs.exists(this.stagingFile)) {
			HadoopUtils.deletePath(this.fs, this.stagingFile, false);
		}

	}

	public String getOutputFilePath() {
		return this.fs.makeQualified(
				new Path(this.properties.getProp(this.outputFilePropName)))
				.toString();
	}
}
