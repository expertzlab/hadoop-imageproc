import java.awt.*;
import java.awt.image.*;

import java.awt.event.*;
import javax.imageio.ImageIO;
import java.io.File;
import java.io.*;
import java.io.IOException;
class TxtToImageSeed  {
    static  int X = 2450, Y = 2450;
    static BufferedImage I;

    static public void main(String[] args){
		
		X = Integer.parseInt(args[0]);
		Y = Integer.parseInt(args[1]);
        int x1=0,y1=0;
        int val[]=new int[1];

        try{

            String line;
            String[] array;

            I = new BufferedImage(X, Y, BufferedImage.TYPE_BYTE_GRAY);
            WritableRaster wr = I.getRaster();

            BufferedReader br = new BufferedReader(new FileReader("xy_cord.txt"));
			int[] whitePixel = new int[]{255};
	
            while ((line = br.readLine()) != null) {
                array  = line.split(",");

                    x1 = Integer.parseInt(array[0].trim());
                    y1 = Integer.parseInt(array[1].trim());
                    val[0] = Integer.parseInt(array[2].trim());
                    wr.setPixel(x1, y1, val);

            }


        Frame f = new Frame( "paint Example" );
        WindowEvent we = new WindowEvent(frm, WindowEvent.WINDOW_CLOSED);
        WindowListener wl = new WindowListener();
		wl.windowClosed(we);

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

    }
    public void mousePressed(MouseEvent e) {

    }
    public void mouseReleased(MouseEvent e) {

    }


}
