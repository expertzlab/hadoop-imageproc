import java.awt.*;
import java.awt.image.*;

import java.awt.event.*;
import javax.imageio.ImageIO;
import java.io.File;
import java.io.*;
import java.io.IOException;
class TxtToImageSeed  {
    static  int X = 450, Y = 450;
    static BufferedImage I;

    static public void main(String[] args){
        int x1=0,y1=0;
        int val[]=new int[1];

        try{

            String line;
            String[] array;

            I = new BufferedImage(X, Y, BufferedImage.TYPE_BYTE_GRAY);
            WritableRaster wr = I.getRaster();

            BufferedReader br = new BufferedReader(new FileReader("xy_cord.txt"));
			int[] whitePixel = new int[]{255};
			
			for(int i=0; i < X; i++){
				for(int j=0; j <Y; j++){
					wr.setPixel(i,j,whitePixel);
					System.out.println("whitePixel:"+whitePixel[0]);
				}
			}

            while ((line = br.readLine()) != null) {
                array  = line.split(",");


                    x1 = Integer.parseInt(array[0]);
                    y1 = Integer.parseInt(array[1]);
                    val[0] = Integer.parseInt(array[2].trim());
                    wr.setPixel(x1, y1, val);



            }


        Frame f = new Frame( "paint Example" );


        f.addMouseListener(new MouseAdapter() {// provides empty implementation of all

            public void mouseClicked(MouseEvent e) {
                System.out.println(e.getX() + "," + e.getY());
            }
            public void mousePressed(MouseEvent e) {
                System.out.println(e.getX() + "," + e.getY());
            }
        });

        f.add("Center", new MainCanvasImage1());
        f.setSize(new Dimension(X,Y+22));

        f.setVisible(true);
        }catch(Exception e){
            System.out.println(e);
        }


    }

}
class MainCanvasImage1 extends Canvas implements MouseListener
{
    PrintWriter writer;
    WritableRaster wr;
    int val=0;

    MainCanvasImage1() {
        addMouseListener(this);
        try {
            writer = new PrintWriter("seed.txt", "UTF-8");
            wr = TxtToImageSeed.I.getRaster();



        }catch (Exception ee){}
    }
    public void paint(Graphics g)
    {
        g.drawImage(TxtToImageSeed.I, 0, 0, Color.red, null);
    }
    public void mouseClicked(MouseEvent e) {
        System.out.println(e.getX() + "," + e.getY());
        val=wr.getSample(e.getX(),e.getY(),0);
        writer.println(e.getX()+","+e.getY()+","+val);

    }
    public void mouseEntered(MouseEvent e) {

    }
    public void mouseExited(MouseEvent e) {
        writer.close();

    }
    public void mousePressed(MouseEvent e) {

    }
    public void mouseReleased(MouseEvent e) {

    }


}
