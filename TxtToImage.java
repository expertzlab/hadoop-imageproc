import java.awt.*;
import java.awt.image.*;
import javax.imageio.ImageIO;
import java.io.File;
import java.io.*;
import java.io.IOException;
class TxtToImage {
    static  int X = 450, Y = 450;
    static BufferedImage I;

    static public void main(String[] args){
        int x1=0,y1=0;
        int val[]=new int[1];

        try{

            String line;
            int j=0;
            String[] array;

            I = new BufferedImage(X, Y, BufferedImage.TYPE_BYTE_GRAY);
            WritableRaster wr = I.getRaster();
            BufferedReader br = new BufferedReader(new FileReader("xy_cord.txt"));

            while ((line = br.readLine()) != null) {
                array  = line.split(",");


                    x1 = Integer.parseInt(array[0]);
                    y1 = Integer.parseInt(array[1]);
                    val[0] = Integer.parseInt(array[2].trim());
                    wr.setPixel(x1, y1, val);



            }
        }catch(Exception e){}

        Frame f = new Frame( "paint Example" );
        f.add("Center", new MainCanvasImage());
        f.setSize(new Dimension(X,Y+22));
        f.setVisible(true);


    }
}
class MainCanvasImage extends Canvas
{
    public void paint(Graphics g)
    {
        g.drawImage(TxtToImage.I, 0, 0, Color.red, null);
    }
}
