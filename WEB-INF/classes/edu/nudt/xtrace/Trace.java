package edu.nudt.xtrace;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.IOException;

public class Trace{
	private ArrayList<Report> reports;
	private ArrayList<Report> edges;
	private ArrayList<Report> roots;
	private String taskID;
	private double delay;
	private Connection conn;
	public ArrayList<Report> getRoots(){return roots;}
	public ArrayList<Report> getReports(){return reports;}
	public Trace(){}
	private void init(Connection conn, String taskID, String delay)
	{
		reports = new ArrayList<Report>();
		edges = new ArrayList<Report>();
		roots = new ArrayList<Report>();
		this.conn = conn;
		this.taskID = taskID;
		try{
			this.delay = Double.parseDouble(delay);
		}catch(Exception e){
			this.delay = Double.MAX_VALUE;
		}
	}
	private boolean loadData()//load reports and edges from mysql
	{
		try{
			PreparedStatement getReportsByTaskID = 
					conn.prepareStatement("select * from Report where TaskID = ?");
			PreparedStatement getEdgesByTaskID = 
					conn.prepareStatement("select * from Edge where TaskID = ?");
		
			getReportsByTaskID.setString(1, taskID);
			ResultSet rs = getReportsByTaskID.executeQuery();
			ResultSetMetaData meta = rs.getMetaData();
			int column1 = meta.getColumnCount();
			while(rs.next())
			{
				Report r = new Report();
				for(int i =1;i<=column1; i++)
					r.put(meta.getColumnName(i).toLowerCase(), rs.getString(i));
				reports.add(r);
			}
			getReportsByTaskID.close();
			rs.close();

			getEdgesByTaskID.setString(1, taskID);
			rs = getEdgesByTaskID.executeQuery();
			meta = rs.getMetaData();
			column1 = meta.getColumnCount();
			while(rs.next())
			{
				Report r = new Report();
				for(int i =1;i<=column1; i++)
					r.put(meta.getColumnName(i).toLowerCase(), rs.getString(i));
				edges.add(r);
			}
			getEdgesByTaskID.close();
			rs.close();
		}catch (SQLException e) {
			return false;
		}
		
		preProcess();
		
		for(int i=0; i<reports.size(); i++)
		{
			Report rep = reports.get(i);
			rep.put("index", String.valueOf(i));
			if(rep.get("starttime")==null || rep.get("endtime")==null)
				continue;
			String st = rep.get("starttime").get(0);
			String et = rep.get("endtime").get(0);
			Long lst=Long.parseLong(st);
			Long let=Long.parseLong(et);
			if(lst==let){
				let=let+1;
				rep.remove("endtime");
				rep.put("endtime",String.valueOf(let));
			}
			double tempDelay = (double)(let- lst)/1000000;
			if(tempDelay > delay)
				rep.put("delay",String.valueOf(delay));
			else
				rep.put("delay",String.valueOf(tempDelay));
		}

		return true;
	}
	
	private void preProcess(){
		for(int i=0; i<reports.size()-1; i++){
			for(int j=i+1; j<reports.size(); j++){
				Report rep1 = reports.get(i);
				Report rep2 = reports.get(j);
				String strRep1 = rep1.toString();
				String strRep2 = rep2.toString();
				if(strRep1.equals(strRep2)){
					reports.remove(j);
					j--;
				}
			}
		}
		for(int i=0; i<edges.size()-1; i++){
			for(int j=i+1; j<edges.size(); j++){
				Report edges1 = edges.get(i);
				Report edges2 = edges.get(j);
				String strEdge1 = edges1.toString();
				String strEdge2 = edges2.toString();
				if(strEdge1.equals(strEdge2)){
					edges.remove(j);
					j--;
				}
			}
		}
	}
	
	private void partTreeFindFather(int j)
	{
		for(int i=0; i<reports.size(); i++)
		{
			if(i == j)
				continue;

			Report father = reports.get(i);
			Report son = reports.get(j);

			if(son.get("tid") == null)
				return;
			if(father.get("tid") == null)
				continue;
			String sid = son.get("tid").get(0);
			String fid = father.get("tid").get(0);
			if(!sid.equals(fid))
				continue;

			if(son.get("starttime")==null || son.get("endtime")==null)
				return;
			long sonST  = Long.parseLong(son.get("starttime").get(0));
			long sonET  = Long.parseLong(son.get("endtime").get(0));
			if(sonST > sonET)
				return;
			if(father.get("starttime")==null || father.get("endtime")==null)
				continue;
			long fatherST  = Long.parseLong(father.get("starttime").get(0));
			long fatherET  = Long.parseLong(father.get("endtime").get(0));
			if(fatherST > fatherET)
				continue;

			if(fatherST <= sonST && sonET <= fatherET)
			{
				if(son.get("father")==null)
					son.put("father", father.get("index").get(0));
				else{
					int existFatherID =  Integer.parseInt(son.get("father").get(0));
					Report existFather = reports.get(existFatherID);
					long existFatherST  = Long.parseLong(existFather.get("starttime").get(0));
					long existFatherET  = Long.parseLong(existFather.get("endtime").get(0));
					if(existFatherST <= fatherST && fatherET <= existFatherET)
					{
						son.remove("father");
						son.put("father", father.get("index").get(0));
					}
				}
			}
			
		}
	}
	private void partTreeFindSon()
	{
		for(int i=0; i<reports.size(); i++)
		{
			Report son = reports.get(i);
			if(son.get("father")==null)
				continue;
			int fatherID = Integer.parseInt(son.get("father").get(0));
			Report father = reports.get(fatherID);
			father.put("children", son.get("index").get(0));
		}
	}
	private void findPartTreeTopology()
	{
		for(int i=0;i<edges.size();i++)
		{
			Report edge = edges.get(i);
			if(edge.get("fathertid")==null || edge.get("fatherstarttime")==null || edge.get("childtid")==null)
				continue;
			String fatherTID = edge.get("fathertid").get(0);
			String fatherST = edge.get("fatherstarttime").get(0);
			String childTID = edge.get("childtid").get(0);
			int fatherIndex = -1;
			for(int j=0; j<reports.size(); j++)
			{
				Report rep = reports.get(j);
				if(rep.get("tid")==null || rep.get("starttime")==null)
					continue;
				String TID = rep.get("tid").get(0);
				String st  = rep.get("starttime").get(0);
				if(fatherTID.equals(TID) && fatherST.equals(st))
				{
					fatherIndex = j;
					break;
				}
			}
			if(fatherIndex == -1)
				continue;
			Report father = reports.get(fatherIndex);
			for(int j=0; j< reports.size(); j++)
			{
				Report son = reports.get(j);
				if(son.get("tid")==null)
					continue;
				String sonTID = son.get("tid").get(0);
				int sonIndex = Integer.parseInt(son.get("index").get(0));
				if(son.get("father")==null && sonTID.equals(childTID))
				{
					son.put("father",String.valueOf(fatherIndex));
					father.put("children", String.valueOf(sonIndex));
				}
			}
		}
	}
	void findRoots()
	{
		for(int i=0;i<reports.size();i++)
		{
			Report rep = reports.get(i);
			if(rep.get("father") == null)
				roots.add(rep);
		}
	}
	private void sortChildren()
	{
		for(int i=0;i<reports.size();i++)
		{
			Report rep = reports.get(i);
			List<String> children = rep.get("children");
			if(children == null) continue;
			for(int j = 0; j<children.size(); j++){
				for(int k = j+1; k<children.size(); k++)
				{
					int indexj = Integer.parseInt(children.get(j));
					int indexk = Integer.parseInt(children.get(k));
					if(indexj>=reports.size() || indexk>=reports.size())
						continue;
					Report sonj = reports.get(indexj);
					Report sonk = reports.get(indexk);
					long startTimej = Long.parseLong(sonj.get("starttime").get(0));
					long startTimek = Long.parseLong(sonk.get("starttime").get(0));
					if(startTimej > startTimek)
					{
						String tmp = children.get(j);
						children.set(j, children.get(k));
						children.set(k, tmp);
					}
				}
			}
			rep.remove("children");
			for(int j=0;j<children.size();j++)
			{
				rep.put("children",children.get(j));
			}
		}

		
		for(int i=0;i<roots.size();i++)
			for(int j=i+1;j<roots.size();j++)
			{
				Report rooti=roots.get(i);
				Report rootj=roots.get(j);
				long startTimei=Long.parseLong(rooti.get("starttime").get(0));
				long startTimej=Long.parseLong(rootj.get("starttime").get(0));
				if(startTimei>startTimej){
					roots.set(i,rootj);
					roots.set(j,rooti);
				}
			}
	}
	public ArrayList<Report> findTopology(Connection conn, String taskID, String delay)
	{
		init(conn, taskID, delay);
		if(!loadData()) return null;
		for(int i=0; i<reports.size(); i++) partTreeFindFather(i);
		partTreeFindSon();
		findPartTreeTopology();
		findRoots();
		sortChildren();
		return reports;
	}
	

	public boolean genTxt(String path, String direction, String shape)
	{
		try {
			File file = new File(path);  
			if(!file.getParentFile().exists()){  
				file.getParentFile().mkdirs();
			}  
			if(!file.exists()){  
				file.createNewFile();
			}  
			
			FileWriter fileWriter = new FileWriter(path,false);  
			BufferedWriter bw = new BufferedWriter(fileWriter);  
			
			
		
			bw.write("digraph G {\n");
			bw.write("\trankdir = " + direction + ";\n");
			bw.write("\tnode [fontsize=\"9\", shape = " + shape + "]\n");
			bw.write("\tedge [fontsize=\"9\"]\n\n");
			for(int i=0;i<roots.size();i++)
				genSubTree(roots.get(i),bw);
			bw.write("}");
			fileWriter.flush();  
			bw.close();  
			fileWriter.close();
		} catch (Exception e) {  
			return false;
		} 
		return true;
	}
	private void genSubTree(Report rep, BufferedWriter bw) throws IOException
	{
		bw.write("\t");
		String d = rep.get("delay").get(0);
		bw.write("\"node_"+rep.get("index").get(0)+"\"");
		//String temp=rep.get("children")==null?"[]":rep.get("children").toString();//
		bw.write(" [label=\""+/**rep.get("index").get(0)+":"+**/rep.get("opname").get(0)+"\\n"+/**temp+"\\n"+**/rep.get("agent").get(0)+"\\n"
			+rep.get("hostname").get(0)+"\\n"+/**rep.get("starttime").get(0)+"\\n"+**/
			String.format("%.2f", Double.parseDouble(d))+"ms\"]\n");
		List<String> children = rep.get("children");
		if(children == null) return;
		for(int j=0; j<children.size(); j++)
		{
			int index = Integer.parseInt(children.get(j));
			genSubTree(reports.get(index),bw);
		}
		for(int j=0; j<children.size(); j++)
		{
			bw.write("\t\t");
			String son = children.get(j);
			bw.write("\"node_"+rep.get("index").get(0)+"\"->\"node_"+son+"\" [color=\"black\"]\n");
		}
		
	}
	public boolean genAbnormalTxt(String path, DiagnosisResult dr)
	{
		String direction = "TB";
		String shape = "ellipse";
		if(dr==null) return genTxt(path, direction, shape);
		if(reports==null)return false;
		if(reports.get(0)==null)return false;
		if(reports.get(0).get("dfsorder")==null)depthFirstSearchOrder();
		try {
			File file = new File(path);  
			if(!file.getParentFile().exists()){  
				file.getParentFile().mkdirs();
			}  
			if(!file.exists()){  
				file.createNewFile();
			}  
			
			FileWriter fileWriter = new FileWriter(path,false);  
			BufferedWriter bw = new BufferedWriter(fileWriter);  
			
			bw.write("digraph G {\n");
			bw.write("\trankdir = " + direction + ";\n");
			bw.write("\tnode [fontsize=\"9\", shape = " + shape + "]\n");
			bw.write("\tedge [fontsize=\"9\"]\n\n");
			for(int i=0;i<roots.size();i++)
				genAbnormalSubTree(roots.get(i),bw,dr);
			bw.write("}");
			fileWriter.flush();  
			bw.close();  
			fileWriter.close();
		} catch (Exception e) {  
			return false;
		} 
		return true;
	}
	private void genAbnormalSubTree(Report rep, BufferedWriter bw,DiagnosisResult dr) throws IOException
	{
		bw.write("\t");
		String d = rep.get("delay").get(0);
		bw.write("\"node_"+rep.get("index").get(0)+"\"");
		String nodeColor = "black";
		ArrayList<Integer> abnormalOps=dr.getAbnormalOperations();
		int op=-1;
		try{op=Integer.parseInt(rep.get("dfsorder").get(0));}catch(Exception e){}
		if(abnormalOps.contains(op))
			nodeColor="red";
		bw.write(" [color=\""+nodeColor+"\" label=\""+rep.get("opname").get(0)+"\\n"+rep.get("agent").get(0)+
				"\\n"+rep.get("hostname").get(0)+"\\n"+String.format("%.2f", Double.parseDouble(d))+"ms");
		if(op>=0 && op<reports.size() && nodeColor.equals("red"))
			bw.write("("+String.format("%.2f", dr.getMinDelay(op))+","+String.format("%.2f", dr.getMaxDelay(op))+")");
		bw.write("\"]\n");
		List<String> children = rep.get("children");
		if(children == null) return;
		for(int j=0; j<children.size(); j++)
		{
			int index = Integer.parseInt(children.get(j));
			genAbnormalSubTree(reports.get(index),bw,dr);
		}
		for(int j=0; j<children.size(); j++)
		{
			String edgeColor = "black";
			bw.write("\t\t");
			String son = children.get(j);
			int index = Integer.parseInt(children.get(j));
			Report sonRep = reports.get(index);
			int sonOp = -1;
			try{sonOp=Integer.parseInt(sonRep.get("dfsorder").get(0));}catch(Exception e){}
			if(nodeColor.equals("red") && abnormalOps.contains(sonOp))
				edgeColor = "red";
			bw.write("\"node_"+rep.get("index").get(0)+"\"->\"node_"+son+"\" [color=\""+edgeColor+"\"]\n");
		}
	}


	private int order=0;
	public void depthFirstSearchOrder()
	{
		order = 0;
		for(int i=0; i<roots.size();i++)
			dfs(roots.get(i),0);
	}
	private void dfs(Report node, int depth)
	{
		node.put("depth",String.valueOf(depth));
		node.put("dfsorder",String.valueOf(order));
		order++;
		List<String> children = node.get("children");
		if(children==null)
			return;
		for(int i=0;i<children.size();i++)
		{
			Report rep = reports.get(Integer.parseInt(children.get(i)));
			dfs(rep,depth+1);
		}
	}
}












