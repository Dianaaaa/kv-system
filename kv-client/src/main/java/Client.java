import com.kvstore.server.MasterService;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class Client {
    public static void main(String[] args) {
        try {
            MasterService hello = (MasterService) Naming.lookup("rmi://localhost:1100/kvMaster");
            hello.PUT("ZJ", "1");
            String response = hello.READ("QXY");

            System.out.println("=======> " + response + " <=======");
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
}
