<%@ page contentType="text/html;charset=UTF-8" %>

<%@ page import="java.util.*"%>
<%@ page import="java.io.File" %>
<%@ page import="java.io.FileReader" %>
<%@ page import="java.io.BufferedReader" %>
<%@ page import="java.io.BufferedWriter" %>
<%@ page import="java.io.FileWriter" %>
<%

String OpType=request.getParameter("OpType");
if(OpType==null || OpType.equals("")){
	return;
}

String temp=application.getRealPath(request.getRequestURI());
String dir=new File(temp).getParent();

if(OpType.equals("saveCurrent"))
{
	String dbname=request.getParameter("dbname");
	if(dbname!=null || !dbname.equals("")){
		File file = new File(dir+"\\database\\"+dbname);
		if(file.exists()){
			out.println("file exists, use other name!");
			return;
		}
	}

	if(dbname!=null || !dbname.equals("")){
		
		File file = new File(dir+"\\tmp\\saveDBcmd.bat");
		if(file.exists())
			file.delete();
		BufferedWriter bw=new BufferedWriter(new FileWriter(file));
		bw.newLine();
		bw.write("mysqldump -u root -proot xtrace > "+dir+"\\database\\"+dbname);
		bw.flush();
		bw.close();
		
		Process p=Runtime.getRuntime().exec(dir+"\\tmp\\saveDBcmd.bat");
		p.waitFor();
	}
}

else if(OpType.equals("change"))
{
	String dbname=request.getParameter("dbname");
        if(dbname!=null || !dbname.equals("")){
		File file = new File(dir+"\\tmp\\changeDBcmd.bat");
		if(file.exists())
                        file.delete();
		BufferedWriter bw=new BufferedWriter(new FileWriter(file));
		bw.write("mysql -u root -proot xtrace < "+dir+"\\database\\"+dbname);
        bw.flush();
        bw.close();
		
		Process p=Runtime.getRuntime().exec(dir+"\\tmp\\changeDBcmd.bat");
		p.waitFor();
	}
}

else if(OpType.equals("rename"))
{
	String oldname=request.getParameter("dbname");
	String newname=request.getParameter("newName");
        if(newname!=null || !newname.equals("")){
                File file = new File(dir+"\\database\\"+newname);
                if(file.exists()){
                        out.println("file exists, use other name!");
                        return;
                }
        }
		

        if(oldname!=null || !oldname.equals("") || newname!=null || !newname.equals("")){
		
				File file = new File(dir+"\\tmp\\renameDBcmd.bat");
				if(file.exists())
		                        file.delete();
				BufferedWriter bw=new BufferedWriter(new FileWriter(file));
				bw.write("move "+dir+"\\database\\"+oldname+" "+dir+"\\database\\"+newname);
		        bw.flush();
		        bw.close();
				
				Process p=Runtime.getRuntime().exec(dir+"\\tmp\\renameDBcmd.bat");
				p.waitFor();
	        }
}

else if(OpType.equals("delete"))
{
	String dbname=request.getParameter("dbname");
        if(dbname!=null || !dbname.equals("")){
		
		File file = new File(dir+"\\tmp\\deleteDBcmd.bat");
		
		BufferedWriter bw=new BufferedWriter(new FileWriter(file));
		bw.write("move "+dir+"\\database\\"+dbname+" "+dir+"\\deletedDB\\"+dbname);
		bw.flush();
		bw.close();
		
		Process p=Runtime.getRuntime().exec(dir+"\\tmp\\deleteDBcmd.bat");
		p.waitFor();
	}
}

else if(OpType.equals("open"))
{
	String dbname=request.getParameter("dbname");
	FileReader fr=new FileReader(dir+"/database/"+dbname);
	BufferedReader br=new BufferedReader(fr);
	String content=br.readLine();
	while(content!=null){
		out.print(content);
		out.print("<p>");
		content=br.readLine();
	}
	br.close();
	fr.close();
	return;
}
String url=request.getHeader("Referer");
if(OpType.equals("change"))
	response.sendRedirect("index.jsp");
else
	response.sendRedirect(url);
%>

