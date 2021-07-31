package org.hps;


import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

public class RLS {
    static private double[][] A;
    static private double[][] B;
    static private int num_vars;
    static private double lamda;

    static private RealMatrix P;

    static private RealMatrix w;
    private static double a_priori_error;


    RLS(int num_vars, double lamda) {
        P = MatrixUtils.createRealIdentityMatrix(num_vars);
        double[][] ww = new double[num_vars][1];
        for (int i = 0; i < num_vars; i++)
            ww[i][0] = 0;
        w = new Array2DRowRealMatrix(ww);
        a_priori_error = 0;

        RLS.num_vars = num_vars;
        RLS.lamda = lamda;
    }

    public RealMatrix getW() {
        return w;
    }

    public void add_obs(RealMatrix X, double t) {
        RealMatrix kn, Pn, k, Pnn;
        double kd, Pd;
        kn = P.multiply(X);
        kd = (((X.transpose()).multiply(P)).multiply(X)).getEntry(0, 0) + lamda;
        k = kn.scalarMultiply(1 / kd);
        Pn = (((P.multiply(X)).multiply(X.transpose())).multiply(P));
        Pd = (((X.transpose()).multiply(P)).multiply(X)).getEntry(0, 0) + lamda;
        Pnn = Pn.scalarMultiply(1 / Pd);
        P = (P.subtract(Pnn)).scalarMultiply((1 / lamda));
        a_priori_error = t - (w.transpose().multiply(X)).getEntry(0, 0);
        w = w.add(k.scalarMultiply(a_priori_error));
    }


    public double get_error() {
        return a_priori_error;
    }


}
