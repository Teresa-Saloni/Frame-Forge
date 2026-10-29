package com.frameforge.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;

/**
 * Tiny helpers to read/write PNG frames through Hadoop's s3a FileSystem, so the same code works
 * against MinIO locally and S3 in the cloud. Configuration comes from spark-defaults.conf
 * (fs.s3a.endpoint, credentials, path-style access).
 */
public final class S3Images {
    private S3Images() {}

    public static void writePng(BufferedImage img, String s3aPath, Configuration conf) throws IOException {
        Path p = new Path(s3aPath);
        FileSystem fs = p.getFileSystem(conf);
        try (FSDataOutputStream out = fs.create(p, true)) {
            ImageIO.write(img, "png", out);
        }
    }

    public static BufferedImage readPng(String s3aPath, Configuration conf) throws IOException {
        Path p = new Path(s3aPath);
        FileSystem fs = p.getFileSystem(conf);
        try (FSDataInputStream in = fs.open(p)) {
            BufferedImage img = ImageIO.read(in);
            if (img == null) throw new IOException("not an image: " + s3aPath);
            return img;
        }
    }

    public static InputStream open(String s3aPath, Configuration conf) throws IOException {
        Path p = new Path(s3aPath);
        return p.getFileSystem(conf).open(p);
    }
}
