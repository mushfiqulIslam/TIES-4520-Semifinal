package endpoint;

import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.any23.Any23;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.HTTPDocumentSource;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.NTriplesWriter;

import org.apache.jena.rdf.model.*;
import org.apache.jena.query.*;
import org.apache.jena.reasoner.*;
import org.apache.jena.reasoner.rulesys.*;

public class JenaEndpointServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        resp.setContentType("text/html;charset=UTF-8");
        PrintWriter out = resp.getWriter();

        String url   = req.getParameter("url");
        String rules = req.getParameter("rules");
        String sparql = req.getParameter("sparql");

        out.println("<html><body>");
        out.println("<h2>Query Result</h2>");

        try {
            // 1. Parse RDFa from XHTML into a Jena Model
            Model model = ModelFactory.createDefaultModel();

            Any23 runner = new Any23();

            // REQUIRED: tell Any23 what User-Agent to use
            runner.setHTTPUserAgent("JenaQR-RDFaEndpoint/1.0 (+https://users.jyu.fi/~musislch/)");

            DocumentSource source =
            		new HTTPDocumentSource(runner.getHTTPClient(), url);


            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            TripleHandler handler = new NTriplesWriter(baos);

            runner.extract(source, handler);
            handler.close();

            ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());
            model.read(in, null, "N-TRIPLES");

            // 2. Apply Jena rules (if provided)
            Model queryModel;
            if (rules != null && !rules.trim().isEmpty()) {
                GenericRuleReasoner reasoner =
                        new GenericRuleReasoner(Rule.parseRules(rules));
                reasoner.setTransitiveClosureCaching(true);

                InfModel infModel = ModelFactory.createInfModel(reasoner, model);
                queryModel = infModel;
            } else {
                queryModel = model;
            }

            Query query = QueryFactory.create(sparql);
            QueryExecution qe = QueryExecutionFactory.create(query, queryModel);

            if (query.isSelectType()) {
                ResultSet rs = qe.execSelect();
                out.println("<table border='1'>");
                out.println("<tr>");
                for (String var : rs.getResultVars()) {
                    out.println("<th>" + var + "</th>");
                }
                out.println("</tr>");
                while (rs.hasNext()) {
                    QuerySolution sol = rs.next();
                    out.println("<tr>");
                    for (String var : rs.getResultVars()) {
                        RDFNode v = sol.get(var);
                        out.println("<td>" + (v == null ? "" : v.toString()) + "</td>");
                    }
                    out.println("</tr>");
                }
                out.println("</table>");
            } else if (query.isAskType()) {
                boolean ans = qe.execAsk();
                out.println("<p>ASK result: <b>" + ans + "</b></p>");
            } else if (query.isConstructType()) {
                Model m = qe.execConstruct();
                out.println("<pre>");
                m.write(out, "TURTLE");
                out.println("</pre>");
            } else if (query.isDescribeType()) {
                Model m = qe.execDescribe();
                out.println("<pre>");
                m.write(out, "TURTLE");
                out.println("</pre>");
            }

            qe.close();

        } catch (Exception e) {
            out.println("<p><b>Error:</b> " + e.getMessage() + "</p>");
            e.printStackTrace(out);
        }

        out.println("</body></html>");
    }
}