package com.shyam.sparkimagevisualizer;

import org.apache.spark.api.java.*;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ImageVisualizer {
	private static final int SQ_LEN = 100;
	private static final int imageHeight = 700;
	private static final int imageWitdth = 500;

	private static JFrame frame;
	private static JLabel label;

	public static void main(String[] args) {
		frame = new JFrame("Reading Images");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		label = new JLabel();
		frame.add(label);
		frame.setVisible(true);

		String imageSnapshotPath = "/imagefiles/.snapshot/";
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String hdfsUri = "hdfs://localhost:9000";
		Configuration hadoopConf = new Configuration();
		hadoopConf.set("fs.defaultFS", hdfsUri);
		
		FileStatus[] snapshotList = null;
		try {
			snapshotList = FileSystem.get(hadoopConf).listStatus(new Path(imageSnapshotPath));
		} catch (IllegalArgumentException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (FileStatus snapshotDir : snapshotList) {

			String imageFileDir = hdfsUri + imageSnapshotPath + snapshotDir.getPath().getName();

			JavaRDD<String> imageData = sc.textFile(imageFileDir).cache();
			JavaPairRDD<String, String> timestampMap = imageData.mapToPair(s -> new Tuple2(s.split(":")[0], s));

			JavaPairRDD<String, Iterable<String>> timestampGroups = timestampMap.groupByKey();

			List<Tuple2<String, Iterable<String>>> collectedGroups = timestampGroups.collect();

			for (Tuple2 tuple : collectedGroups) {
				BufferedImage bufferedImage = new BufferedImage(imageWitdth, imageHeight,
						BufferedImage.TYPE_INT_ARGB);
				String imageName = tuple._1.toString();
				Iterable<String> pixelLineList = (Iterable<String>) tuple._2;
				for (String pixelLine : pixelLineList) {
					String[] pixelValues = pixelLine.split(":");
					int sq_index = parseString(pixelValues[1]);

					int x_offset = (sq_index % 5) * SQ_LEN;
					int y_offset = (sq_index / 5) * SQ_LEN;

					int x_index, y_index;

					int pixel_index = 2;

					for (int y = 0; y < SQ_LEN; y++) {
						y_index = y_offset + y;
						for (int x = 0; x < SQ_LEN; x++) {
							x_index = x_offset + x;
							try {
								bufferedImage.setRGB(x_index, y_index, parseString(pixelValues[pixel_index++]));
							} catch (Exception e) {
								e.printStackTrace();
								int l = pixelValues[pixel_index - 1].trim().length();
								System.out.println(l);
								for (int i = 0; i < l; i++) {
									System.out.println(pixelValues[pixel_index - 1].charAt(i));
								}
								System.out.println(pixelValues[pixel_index - 1]);
							}
						}
					}
					updateImage(bufferedImage);
				}
				Graphics g = bufferedImage.getGraphics();
				g.setColor(new Color(255, 0, 0, 127));
				g.fillOval(100, 100, 50, 50);
				updateImage(bufferedImage);
			}
		}
		sc.stop();
	}

	private static void updateImage(BufferedImage image) {
		ImageIcon imageIcon = new ImageIcon(image);
		label.setIcon(imageIcon);
		frame.pack();
	}

	private static int parseString(String line) {
		String result = "";
		for (int i = 1; i < line.length(); i += 2) {
			result += line.charAt(i);
		}
		return Integer.parseInt(result);
	}

}
