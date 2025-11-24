package com.musislch.ties4520;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;

import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.parser.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.rio.*;

@WebServlet(name = "SPARQLEndpointServlet", urlPatterns = {"/api/query"})
public class SPARQLEndpointServlet extends HttpServlet {

  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    req.setCharacterEncoding("UTF-8");
    String repoUrl = param(req, "repoUrl", "http://localhost:8080/rdf4j-server");
    String repoId  = param(req, "repoId",  "urban-mobility-individuals");
    String query   = req.getParameter("query");

    if (query == null || query.isBlank()) {
      resp.setStatus(400);
      resp.setContentType("text/plain; charset=UTF-8");
      resp.getWriter().println("Missing 'query' parameter.");
      return;
    }

    RemoteRepositoryManager manager = null;
    try {
      try {
        manager = RemoteRepositoryManager.getInstance(repoUrl);
      } catch (NoSuchMethodError | NoClassDefFoundError e) {
        manager = new RemoteRepositoryManager(repoUrl);
      }

      Repository repo = manager.getRepository(repoId);
      if (repo == null) {
        resp.setStatus(404);
        resp.setContentType("text/plain; charset=UTF-8");
        resp.getWriter().println("Repository not found: " + repoId);
        return;
      }

      ParsedOperation parsed = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, null);
      try (RepositoryConnection conn = repo.getConnection()) {

        if (parsed instanceof ParsedTupleQuery) {
          TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
          resp.setContentType("application/sparql-results+json; charset=UTF-8");
          tq.evaluate(new org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter(resp.getOutputStream()));

        } else if (parsed instanceof ParsedBooleanQuery) {
          boolean b = conn.prepareBooleanQuery(QueryLanguage.SPARQL, query).evaluate();
          resp.setContentType("application/sparql-results+json; charset=UTF-8");
          resp.getOutputStream().write(("{\"boolean\":" + b + "}").getBytes(StandardCharsets.UTF_8));

        } else if (parsed instanceof ParsedGraphQuery) {
          GraphQuery gq = conn.prepareGraphQuery(QueryLanguage.SPARQL, query);
          resp.setContentType("text/turtle; charset=UTF-8");
          OutputStream out = resp.getOutputStream();
          RDFWriter w = Rio.createWriter(RDFFormat.TURTLE, out);
          gq.evaluate(w);

        } else {
          resp.setStatus(400);
          resp.setContentType("text/plain; charset=UTF-8");
          resp.getWriter().println("Unsupported SPARQL operation.");
        }
      }
    } catch (Exception e) {
      resp.setStatus(500);
      resp.setContentType("text/plain; charset=UTF-8");
      e.printStackTrace(resp.getWriter());
    } finally {
      if (manager != null) {
        try { manager.shutDown(); } catch (Throwable ignore) {}
      }
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setContentType("text/plain; charset=UTF-8");
    resp.getWriter().println("POST a SPARQL query to this endpoint.");
  }

  private static String param(HttpServletRequest r, String n, String d) {
    String v = r.getParameter(n);
    return (v == null || v.isBlank()) ? d : v;
  }
}