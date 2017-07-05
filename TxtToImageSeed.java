import java.awt.*;
import java.awt.image.*;

import java.awt.event.*;
import java.io.*;
class TxtToImageSeed  {
    static  int X = 450, Y = 450;
    static BufferedImage I;
    private static PrintWriter writer;

    static public void main(String[] args){

		if(args.length < 2){
		    System.out.println("The size of the image is set as "+X+"x"+Y);
            System.out.println("If it is different please pass it as parameters");
        } else {
            X = Integer.parseInt(args[0]);
            Y = Integer.parseInt(args[1]);
        }
        int x1=0,y1=0;
        int val[]=new int[1];

        try{

            String line;
            String[] array;

            I = new BufferedImage(X, Y, BufferedImage.TYPE_BYTE_GRAY);
            WritableRaster wr = I.getRaster();

            BufferedReader br = new BufferedReader(new FileReader("xy_cord.txt"));

            while ((line = br.readLine()) != null) {
                array  = line.split(",");

                    x1 = Integer.parseInt(array[0].trim());
                    y1 = Integer.parseInt(array[1].trim());
                    val[0] = Integer.parseInt(array[2].trim());
                    wr.setPixel(x1, y1, val);

            }


        Frame f = new Frame( "paint Example" );
        //WindowEvent we = new WindowEvent(f, WindowEvent.WINDOW_CLOSED);
        writer = new PrintWriter("seed.txt", "UTF-8");
        WindowListener wl = new TxtImgWindowListener(writer);
        f.addWindowListener(wl);
		//wl.windowClosed(we);

        f.addMouseListener(new MouseAdapter() {// provides empty implementation of all

            public void mouseClicked(MouseEvent e) {
                System.out.println(e.getX() + "," + e.getY());
            }
            public void mousePressed(MouseEvent e) {
                System.out.println(e.getX() + "," + e.getY());
            }
        });

        f.add("Center", new MainCanvasImage1(writer));
        f.setSize(new Dimension(X,Y+22));

        f.setVisible(true);
        }catch (ArrayIndexOutOfBoundsException e){
            System.out.println("Size of the images can be bigger that the set canvas");
            System.out.println("Please pass the size of the image as");
            System.out.println("java TxtToImageSeed <width> <height>");
        } catch (FileNotFoundException e){
            System.out.println("The file xy_cord.txt does not exists");
            System.out.println("Please use program: ImgToText <jpg/png fileName>");
            System.out.println("To run TxtToImageSeed Program");
        } catch (UnsupportedEncodingException e){
            System.out.println("Image file Format is not supported.");
        } catch(IOException e){
            System.out.println("File read write error.");
        }


    }

}

class TxtImgWindowListener implements WindowListener{

    PrintWriter writer;

    TxtImgWindowListener(PrintWriter writer){
        this.writer = writer;
    }

    @Override
    public void windowOpened(WindowEvent windowEvent) {

    }

    @Override
    public void windowClosing(WindowEvent windowEvent) {
        System.out.println("Closed Detected");
        writer.flush();
        writer.close();
        System.out.println("File Closed");
        System.out.println("Window Closed");
        System.exit(0);
    }

    @Override
    public void windowClosed(WindowEvent windowEvent) {

    }

    @Override
    public void windowIconified(WindowEvent windowEvent) {

    }

    @Override
    public void windowDeiconified(WindowEvent windowEvent) {

    }

    @Override
    public void windowActivated(WindowEvent windowEvent) {

    }

    @Override
    public void windowDeactivated(WindowEvent windowEvent) {

    }
}

class MainCanvasImage1 extends Canvas implements MouseListener
{
    PrintWriter writer;
    WritableRaster wr;
    int val=0;

    MainCanvasImage1(PrintWriter writer) {
        addMouseListener(this);
        try {
            this.writer = writer;
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
