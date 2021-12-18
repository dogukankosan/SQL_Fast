using System;
using System.Data.SqlClient;
using System.IO;
namespace Sql_Fast
{
    class Program
    {
        static void Main()
        {
            try
            {
                Console.Write($"ETA HIZLANDIRMA İŞLEMİ BAŞLADI LÜTFEN BEKLEYİNİZ...!!");
                // ALL STATISTICS UPDATE
                string[] databasename = new string[255];
                byte databasecount = default;
                SqlConnection myConnection = new(File.ReadAllText("connect.txt"));
                myConnection.Open();
                SqlCommand command = new(" select name  from sys.databases where name like 'ETA_%'", myConnection);
                SqlDataReader read = command.ExecuteReader();
                while (read.Read())
                {
                    databasename[databasecount] = read[0].ToString();
                    databasecount++;
                }
                Array.Resize(ref databasename, databasecount - 1);
                myConnection.Close();
                for (byte i = default; i < databasename.Length; i++)
                {
                    myConnection.Open();
                    SqlCommand command2 = new($"use {databasename[i]}  declare @degisken nvarchar(500) declare @tableName nvarchar(200) declare crs cursor for select '[' + TABLE_SCHEMA + ']' + '.' + '[' + TABLE_NAME + ']' from INFORMATION_SCHEMA.TABLES open crs fetch next from crs into @tableName while @@FETCH_STATUS = 0 begin set @degisken = 'UPDATE STATISTICS' + @tableName exec sp_executesql @degisken fetch next from crs into @tableName end close crs deallocate crs", myConnection);
                    command2.ExecuteNonQuery();
                    myConnection.Close();
                }
                myConnection.Open();
                // ALL INDEX UPDATE
                SqlCommand command3 = new($"DECLARE @Database VARCHAR(255)   \r\nDECLARE @Table VARCHAR(255)  \r\nDECLARE @cmd NVARCHAR(500)  \r\nDECLARE @fillfactor INT\r\nSET @fillfactor = 10 \r\nDECLARE DatabaseCursor CURSOR FOR \r\nSELECT name FROM master.dbo.sysdatabases   \r\nWHERE name NOT IN ('master','msdb','tempdb','model','distribution')  and name like 'ETA_%'\r\nORDER BY 1  \r\nOPEN DatabaseCursor  \r\nFETCH NEXT FROM DatabaseCursor INTO @Database \r\nWHILE @@FETCH_STATUS = 0  \r\nBEGIN \r\n   SET @cmd = 'DECLARE TableCursor CURSOR FOR SELECT ''['' + table_catalog + ''].['' + table_schema + ''].['' + \r\n  table_name + '']'' as tableName FROM [' + @Database + '].INFORMATION_SCHEMA.TABLES \r\n  WHERE table_type = ''BASE TABLE'''  \r\n   EXEC (@cmd)  \r\n   OPEN TableCursor   \r\n   FETCH NEXT FROM TableCursor INTO @Table  \r\n   WHILE @@FETCH_STATUS = 0   \r\n   BEGIN  \r\n       IF (@@MICROSOFTVERSION / POWER(2, 24) >= 9)\r\n       BEGIN\r\n           SET @cmd = 'ALTER INDEX ALL ON ' + @Table + ' REBUILD WITH (FILLFACTOR = ' + CONVERT(VARCHAR(3),@fillfactor) + ')'\r\n           EXEC (@cmd) \r\n       END\r\n       ELSE\r\n       BEGIN\r\n          DBCC DBREINDEX(@Table,' ',@fillfactor)  \r\n       END\r\n       FETCH NEXT FROM TableCursor INTO @Table  \r\n   END   \r\n   CLOSE TableCursor   \r\n   DEALLOCATE TableCursor  \r\n   FETCH NEXT FROM DatabaseCursor INTO @Database \r\nEND \r\nCLOSE DatabaseCursor   \r\nDEALLOCATE DatabaseCursor", myConnection);
                command3.ExecuteNonQuery();
                myConnection.Close();
                Console.Clear();
                Console.Write("ETA HIZLANDIRMA İŞLEMİ BAŞARILI UYGULAMAYI KAPATABİLİRSİNİZ !!");
            }
            catch (Exception a)
            {
                Console.Write($"ETA HIZLANDIRMA İŞLEMİ BAŞARISIZ {a} !!");

            }
            Console.ReadKey();
        }
    }
}
