import java.sql.*;


public class FTM {
	public static void main(String args[]) throws SQLException, ClassNotFoundException{
		try
		{
		boolean testmode = false;
		
		if (args.length != 2)
			{
			System.out.println("Error, invalid # of arguments");
			System.out.println("Usage: FTM username password");
			throw new IllegalArgumentException();
			}

		//Loads PostgreSQL driver
		Class.forName("org.postgresql.Driver");
		
		//Connect to the local database
		Connection conn = DriverManager.getConnection 
				("jdbc:postgresql://localhost:5432/pa2",
				args[0], args[1]);
		
		//Start the program by dropping the influence table
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("drop table if exists influence;");
		if(testmode){System.out.println("Dropped influence table");}	

		
		//Create influence(from,to) table
		stmt.executeUpdate("create table influence( \"from\" text NOT NULL, \"to\" text NOT NULL)");
		if(testmode){System.out.println("Created influence table");}		
		
		
		//Create current table T:= G
		stmt.executeUpdate("drop table if exists current;");
		stmt.executeUpdate("create table current(\"from\" text, \"to\" text)");
		stmt.executeUpdate("insert into current(\"from\", \"to\") select src, tgt from transfer");
		if(testmode){System.out.println("Created current table");}		

		
		//Create Delta table Δ := G
		stmt.executeUpdate("drop table if exists delta;");
		stmt.executeUpdate("create table delta(\"from\" text, \"to\" text)");
		stmt.executeUpdate("insert into delta(\"from\", \"to\") select src, tgt from transfer");
		if(testmode){System.out.println("Created delta table");}		
		
		//Create Previous table T_old
		stmt.executeUpdate("drop table if exists previous;");
		stmt.executeUpdate("create table previous(\"from\" text, \"to\" text)");
		stmt.executeUpdate("insert into previous(\"from\", \"to\") select src, tgt from transfer");
		if(testmode){System.out.println("Created previous table");}	
		
		
		
		/*
		//Insert a test tuple for delta
		stmt.executeUpdate("insert into delta(\"from\",\"to\") values (\'9\', \'10\')");
		if(testmode){System.out.println("Inserted a tuple");}	
		*/
		
		//whileΔ ≠ ∅ do
		ResultSet rset = stmt.executeQuery("select count(*) from delta");
		int sizeOfDelta = 0;
		if (rset.next())
		{
		sizeOfDelta = rset.getInt(1);
		}
		if(testmode){System.out.println("sizeOfDelta is: " + sizeOfDelta);}	
		
		while(sizeOfDelta > 0){
			//T_old =T
			stmt.executeUpdate("delete from previous");
			stmt.executeUpdate("insert into previous select * from current");
			if(testmode){System.out.println("1");}	

			//T := (select * from T)
			//		union
			//		(select x.A, y.B from Δ x, T y where x.B = y.A)
			//		union
			//		(select x.A,y.B fromTx,Δy where x.B = y.A)
			stmt.executeUpdate("delete from current");
			stmt.executeUpdate("insert into current (select * from previous) union (select x.from, y.to from delta as x, previous as y where x.to = y.from) union (select x.from, y.to from previous as x, delta as y where x.to = y.from)");
			if(testmode){System.out.println("2");}	
			
			//		Δ := T – Told }
			stmt.executeUpdate("delete from delta");
			stmt.executeUpdate("insert into delta select * from current except select * from previous");
			if(testmode){System.out.println("4");}	
			
			
			rset = stmt.executeQuery("select count(*) from delta");
	
			if (rset.next())
			{
			sizeOfDelta = rset.getInt(1);
			}
			if(testmode){System.out.println("sizeOfDelta is: " + sizeOfDelta);}	

		}
		
		//Execute query asking for all info from influence
		stmt.executeUpdate("insert into influence select * from current order by \"to\"");
		rset = stmt.executeQuery("select * from influence order by \"from\"");
		if(testmode){System.out.println("Ran select from influence");}	

		//Print result of influence
		//Output T

		if(testmode){System.out.println("Going to print final result");}	
		while(rset.next() && testmode)
		{
			System.out.println(rset.getString(1) + " | " + rset.getString(2));
					//" | " + rset.getString(3)+ " | " + rset.getString(4));
		}
		
		
		//End by dropping all auxiliary tables you may have created. 
		
		stmt.executeUpdate("drop table if exists current");
		stmt.executeUpdate("drop table if exists previous");
		stmt.executeUpdate("drop table if exists delta");
		
		//Close the result set, statement, and connection
		rset.close();
		stmt.close();
		conn.close();
		
		
		}catch(SQLException ex)
		{
			System.err.println("SQL Exception: "+ ex.getMessage());
		}
		
	}	
}
