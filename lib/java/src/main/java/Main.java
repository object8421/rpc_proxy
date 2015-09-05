import me.chunyu.rpc_proxy.GeneralRpcServer;
import org.apache.thrift.TException;
import service.chunyu.demo.echo.EchoService;

public class Main {


    public static class Processor implements EchoService.Iface {
        @Override
        public String echo(String msg) throws TException {
            return "Hello: " + msg;
        }

        @Override
        public void ping() throws TException {
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        EchoService.Processor processor = new EchoService.Processor(new Processor());
        GeneralRpcServer server = new GeneralRpcServer(processor, "config.ini");
        server.serve();

    }
}
