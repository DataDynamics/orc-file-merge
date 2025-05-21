package io.datadynamics.ibk.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;

public class MergeOrcFiles {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: MergeOrcFiles <inputDir> <outputFile>");
            System.exit(1);
        }
        String inputDir = args[0];      // e.g., hdfs:///a
        String outputFile = args[1];    // e.g., hdfs:///merged_orc_output/output.orc

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("hadoop.security.authentication", "simple");
        conf.set("hadoop.security.authorization", "false");

        UserGroupInformation.setConfiguration(conf);
        FileSystem fs = FileSystem.get(conf);

        // 재귀적으로 ORC 파일 목록 조회
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(new Path(inputDir), true);

        Writer writer = null;
        while (fileIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileIterator.next();
            Path filePath = fileStatus.getPath();

            if (!filePath.getName().endsWith(".orc")) {
                continue;
            }

            // ORC 리더 생성
            Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));

            if (writer == null) {
                // 첫 번째 파일 스키마로 Writer 초기화
                TypeDescription schema = reader.getSchema();
                writer = OrcFile.createWriter(new Path(outputFile),
                        OrcFile.writerOptions(conf).setSchema(schema));
            }

            // 레코드 읽어서 Writer에 추가
            RecordReader rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            while (rows.nextBatch(batch)) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            rows.close();
        }

        if (writer != null) {
            writer.close();
        }
        fs.close();
    }

}