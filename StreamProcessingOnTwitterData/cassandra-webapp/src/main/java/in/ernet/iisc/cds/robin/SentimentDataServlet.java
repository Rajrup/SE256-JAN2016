package in.ernet.iisc.cds.robin;

import org.json.JSONArray;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by robin on 30/4/16.
 */
@WebServlet(name = "SentimentDataServlet",value = "/sentimentdata.json")
public class SentimentDataServlet extends HttpServlet {
    private final Cassandra client;

    public SentimentDataServlet() {
        super();
        client=new Cassandra();
        client.connect("127.0.0.1");
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String totalString=request.getParameter("total");
        boolean total=((totalString!=null&&(totalString.equals("true")))?true:false);
        JSONArray array;
        if(total)
            array=client.getTotal();
        else
            array = client.getData();
        response.setContentType("application/json");
        response.getWriter().write(array.toString());
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request,response);
    }
}
