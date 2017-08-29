package com.shyam.sparkimagevisualizer;

import org.apache.spark.api.java.*;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import javax.imageio.ImageIO;
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
	private static final String BASE_IMAGE_PATH = "res/base_image.png";
	private static final String COORDINATES_FILE = "res/coordinates.txt";
	
	private static final int SQ_LEN = 100;
	private static final int imageHeight = 1000;
	private static final int imageWidth = 1000;

	private static JFrame frame;
	private static JLabel label;

	public static void main(String[] args) {
		Random random = new Random();
		
		frame = new JFrame("Reading Images");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		label = new JLabel();
		frame.add(label);
		frame.setSize(imageWidth, imageHeight);
		frame.setVisible(true);
		
		BufferedImage baseImage = null;
		try {
			baseImage = ImageIO.read(new File(BASE_IMAGE_PATH));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
		updateImage(baseImage);
		
		
		HashMap<Integer, List<Integer>> coordinateMap = findCoordinates();
		
//		for (int i = 0; i < 35; i++) {
//			BufferedImage frame = copyImage(baseImage);
//			for (int key : coordinateMap.keySet()) {
//				int x = coordinateMap.get(key).get(0);
//				int y = coordinateMap.get(key).get(1);
//				Graphics g = frame.getGraphics();
//				drawContour(g, x, y, random.nextInt(256), random.nextInt(256));
//			}
//			updateImage(frame);
//		}
		
		String imageSnapshotPath = "/imagefiles/.snapshot/";
		SparkConf conf = new SparkConf().setAppName("Visualization Application");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String hdfsUri = "hdfs://localhost:9000";
		Configuration hadoopConf = new Configuration();
		hadoopConf.set("fs.defaultFS", hdfsUri);
		
		HashMap<Integer, Float> magnitudeMap = new HashMap<Integer, Float>();
		HashMap<Integer, Float> phaseMap = new HashMap<Integer, Float>();
		
		
		FileStatus[] snapshotList = null;
		try {
			snapshotList = FileSystem.get(hadoopConf).listStatus(new Path(imageSnapshotPath));
		} catch (IllegalArgumentException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (FileStatus snapshotDir : snapshotList) {

			BufferedImage frame = copyImage(baseImage);
			Graphics frameGraphics = frame.getGraphics();
			
			String imageFileDir = hdfsUri + imageSnapshotPath + snapshotDir.getPath().getName();

			JavaRDD<String> imageData = sc.textFile(imageFileDir).cache();
			JavaPairRDD<String, String> timestampMap = imageData.mapToPair(s -> new Tuple2(s.split(":")[0], s));

			JavaPairRDD<String, Iterable<String>> timestampGroups = timestampMap.groupByKey();

			List<Tuple2<String, Iterable<String>>> collectedGroups = timestampGroups.collect();

			for (Tuple2 tuple : collectedGroups) {
				Iterable<String> phasorLineList = (Iterable<String>) tuple._2;
				for (String phasorLine : phasorLineList) {
					String[] phasorValues = phasorLine.split(":");
					
					float magnitude = findFloat(phasorValues[2]);
					float phase = findFloat(phasorValues[3]);
					
					int pmuIndex = findInt(phasorValues[1]);
					if (magnitudeMap.containsKey(pmuIndex)) {
						
						float originalMagnitude = magnitudeMap.get(pmuIndex);
						float originalPhase = phaseMap.get(pmuIndex);
						
						float magnitudeChange = magnitude - originalMagnitude;
						float phaseChange = (phase - originalPhase)/2;
						
						int radius = (int) magnitudeChange / 10000;
						if (radius < 0) {
							radius *= -1;
						}
						
						int red = 0, green = 0;
						if (phaseChange > 0){
							green = (int) (phaseChange * 256);
							if (green > 256) {
								green = 256;
							}
						} else {
							red = (int) (phaseChange * 256 * -1);
							if (red > 256) {
								red = 256;
							}
						}

						drawContour(frameGraphics, coordinateMap.get(pmuIndex).get(0),
								coordinateMap.get(pmuIndex).get(1), red, green, radius);
					} else {
						magnitudeMap.put(pmuIndex, magnitude);
						phaseMap.put(pmuIndex, phase);
					}
					
				}
//				Graphics g = bufferedImage.getGraphics();
//				g.setColor(new Color(255, 0, 0, 127));
//				g.fillOval(100, 100, 50, 50);
//				updateImage(bufferedImage);
				updateImage(frame);
			}
		}
		sc.stop();
	}

	private static void updateImage(BufferedImage image) {
		Image scaledImage = image.getScaledInstance(imageWidth, -1, Image.SCALE_SMOOTH);
		
		ImageIcon imageIcon = new ImageIcon(scaledImage);
		label.setIcon(imageIcon);
		frame.setSize(700, 700);
		frame.pack();
		
	}

	private static int findInt(String line) {
		String result = "";
		for (int i = 1; i < line.length(); i += 2) {
			result += line.charAt(i);
		}
		return Integer.parseInt(result);
	}
	
	private static float findFloat(String line) {
		String result = "";
		for (int i = 1; i < line.length(); i += 2) {
			result += line.charAt(i);
		}
		return Float.parseFloat(result);
	}
	
	public static BufferedImage copyImage(BufferedImage source){
	    BufferedImage b = new BufferedImage(source.getWidth(), source.getHeight(), source.getType());
	    Graphics g = b.getGraphics();
	    g.drawImage(source, 0, 0, null);
	    g.dispose();
	    return b;
	}
	
	private static void drawContour(Graphics g, int x, int y, int red, int green, int radius) {
		
		g.setColor(new Color(red, green, 0, 63));
		
		int r = radius;
		g.fillOval(x - r/2, y - r/2, r, r);
		r = 2 * radius;
		g.fillOval(x - r/2, y - r/2, r, r);
		r = 4 * radius;
		g.fillOval(x - r/2, y - r/2, r, r);
	}
	
	public static HashMap<Integer, List<Integer>> findCoordinates() {
		
		HashMap<Integer, List<Integer>> coordinateMap = new HashMap<Integer, List<Integer>>();
		String line;
		try {
			InputStream fis = new FileInputStream(COORDINATES_FILE);
			InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
		    BufferedReader br = new BufferedReader(isr);
		
			while ((line = br.readLine()) != null) {
			    String[] lineSplit = line.split("\\s+");
			    int index = Integer.valueOf(lineSplit[0].trim());
			    List<Integer> coordinates = new ArrayList<Integer>();
			    coordinates.add(Integer.valueOf(lineSplit[1].trim()));
			    coordinates.add(Integer.valueOf(lineSplit[2].trim()));
			    coordinateMap.put(index, coordinates);
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return coordinateMap;
	}

}
