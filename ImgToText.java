/**
 * Created by geo on 9/6/17.
 */

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.*;
import java.io.IOException;
import javax.imageio.ImageIO;

public class ImgToText {

    public static void main(String[] args) {
        try {
            byte[] imageInByte;
            int k=0;
            BufferedImage originalImage = ImageIO.read(new File(
                    args[0]));

            int width=originalImage.getWidth();
            int height=originalImage.getHeight();
            Raster raster = originalImage.getData();
            int[][] imgArr = new int[width][height];
            PrintWriter writer = new PrintWriter("xy_cord.txt", "UTF-8");
            writer.println(width+","+height+","+"");

            // convert BufferedImage to byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(originalImage, "png", baos);
            baos.flush();
            imageInByte = baos.toByteArray();
            baos.close();

            for (int i = 0; i < width; i++) {
                for (int j = 0; j < height; j++) {
                    imgArr[i][j] = raster.getSample(i, j, 0);
                    writer.println(i+","+j+","+imgArr[i][j]);
                }
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}