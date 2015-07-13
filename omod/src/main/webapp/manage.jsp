<%@ include file="/WEB-INF/template/include.jsp"%>
<%@ include file="/WEB-INF/template/header.jsp"%>

<%@ include file="template/localHeader.jsp"%>

<p>Hello ${user.systemId}!</p>
<a href="<openmrs:contextPath/>/moduleServlet/ohdsi/downloadInsertStatementsServlet">downloadfile</a>

<%@ include file="/WEB-INF/template/footer.jsp"%>