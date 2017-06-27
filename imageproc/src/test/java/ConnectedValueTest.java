import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class ConnectedValueTest{


    @Test
    public void calcConnectedValueTo() throws IOException, InterruptedException {

        int m1 = 120;
        int m2 = 125;
        float s1 = 25;
        float s2 = 10;

        int seedIntensity = 20;
        int pixelIntensity = 225;
        System.out.println("SeedIntensity = "+ seedIntensity+ ", pixelIntensity ="+pixelIntensity);

        double term1 = 0.5*(seedIntensity + pixelIntensity);
        System.out.println("Term 1 ="+ term1);
        double term2 =  ((term1- m1)/(2*s1));
        System.out.println("Term 2 ="+ term2);
        double term3 = - Math.pow(term2,2);
        System.out.println("Term 3 ="+ term3);
        double g1 = Math.exp(term3);
        System.out.println("g1 ="+ g1);

        double termA = 0.5*(seedIntensity - pixelIntensity);
        System.out.println("Term A ="+ termA);
        double termB =  ((termA- m1)/(2*s1));
        System.out.println("Term B ="+ termB);
        double termC = - Math.pow(termB,2);
        System.out.println("Term C ="+ termC);
        double g2 = Math.exp(termC);
        System.out.println("g2 ="+ g2);

        //float w1 = (float) (g1 / (g1+g2));
        //float w2 = 1 - w1;
        float w1 = 0.5f;
        float w2 = 0.5f;
        float mu = (float) (w1 * g1 + w2 * g2);
        System.out.println("MU =" + mu);

    }

}