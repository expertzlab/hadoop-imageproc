import java.awt.*;
import java.awt.image.*;
import javax.imageio.ImageIO;
import java.io.File;
import java.io.*;
import java.io.IOException;
class TxtToImage {
    static  int X = 380, Y = 250;
    static BufferedImage I;

    static public void main(String[] args){
        int x1=0,y1=0;
        int val[]=new int[3];
	System.out.println("Starts loading...");
        try{
            BufferedReader br = new BufferedReader(new FileReader("xy_cord.txt"));
            String line;
            int j=0;
            String[] array;

            if ((line = br.readLine()) != null) {
                array = line.split(",");
                X = Integer.parseInt(array[0]);
                Y = Integer.parseInt(array[1]);
            }
            j=0;
            I = new BufferedImage(X, Y, BufferedImage.TYPE_BYTE_GRAY);
            WritableRaster wr = I.getRaster();
            br = new BufferedReader(new FileReader("xy_cord.txt"));

            while ((line = br.readLine()) != null) {
                array  = line.split(",");
                if(j== 0) {
                    x1 = Integer.parseInt(array[0]);
                    y1 = Integer.parseInt(array[1]);

                } else {

                    x1 = Integer.parseInt(((String)array[0]).trim());
                    y1 = Integer.parseInt(((String)array[1]).trim());
                    val[0] = (int) Float.parseFloat(array[2]);
                    wr.setPixel(x1, y1, val);
		    System.out.print("set-");
		    System.out.println(String.format("%d,%d,%d",x1,y1,val[0]));
                }
                j++;

            }
        }catch(Exception e){
	  System.out.println("Exception in program\n");
	  e.printStackTrace();
	}

        Frame f = new Frame( "paint Example" );
        f.add("Center", new MainCanvasImage());
        f.setSize(new Dimension(X,Y+22));
        f.setVisible(true);

        for(int q=0; q < args.length; ++q) {
            int z = args[q].lastIndexOf('.');
            if (z > 0) try {ImageIO.write(
                    I, args[q].substring(z+1),
                    new File(args[q]));}
            catch (IOException e) {
                System.err.println("image not saved.");
            }}
    }
}
class MainCanvasImage extends Canvas
{
    public void paint(Graphics g)
    {
        g.drawImage(TxtToImage.I, 0, 0, Color.red, null);
    }
}
