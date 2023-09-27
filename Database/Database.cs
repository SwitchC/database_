using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database
{
    class Database
    {
        private string _path;
        public string _name;
        public List<Table> _tables = new List<Table>();
        public Database(string path, string name)
        {
            _path = path + "\\" + name;
            this._name = name;
        }
        public void create()
        {
            Directory.CreateDirectory(_path);
        }
        public string getPath() { return _path; }
        public Table? getTable(string name)
        {
            return _tables.Find(t => t.getName() == name);
        }
        public void createTable(string arg)
        {
            string[] args = arg.Split(" ");
            var table = new Table(args[0], this._path, args[1]);
            table.create();
            _tables.Add(table);
        }
        public void deleteTable(string name)
        {
            Table? table = _tables.Find(t => t.getName() == name);
            table?.delete();
            if (table is not null) _tables.Remove(table);
        }
        public void open()
        {
            string[] directories = Directory.GetDirectories(this._path);
            foreach (var s in directories)
            {
                string[] sepPath = s.Split("\\");
                Table table = new Table(sepPath[sepPath.Length - 1], _path);
                table.open();
                _tables.Add(table);
            }
        }
        public bool union(Table table1, Table table2, string nameNewTable)
        {
            if (!Table.compareTables(table1, table2)) return false;
            createTable(nameNewTable + " " + table1.getSchemaStr());
            Table newTable = getTable(nameNewTable);
            List<string> rowsTable1 = table1.getAllRows();
            int id = 0;
            string? value = table2.getRow(id);
            while (value is not null)
            {
                if (rowsTable1.Contains(value))
                {
                    id++;
                    value = table2.getRow(id);
                    continue;
                }
                else
                {
                    newTable.addRow(value);
                    id++;
                    value = table2.getRow(id);
                }
            }
            foreach (string row in rowsTable1)
            {
                newTable.addRow(row);
            }
            return true;
        }
        public void delete() {
            Directory.Delete(_path, true);
        }
        public Table? joinTable(Table table1, Table table2, string column, string nameNewTable) {
            Table? newTable = Table.join(table1,table2,column,nameNewTable,_path);
            if(newTable is not null) _tables.Add(newTable);
            return newTable;
        }
    }
    class Table
    {

        private string _name;
        private string _path;
        private List<Column> _schema = new List<Column>();
        public Table(string name, string path)
        {
            this._name = name;
            this._path = path + "\\" + name;
        }
        public List<Column> getSchema() { return _schema; }
        public string getSchemaStr()
        {
            string schemaStr = "";
            for (int i = 0; i < _schema.Count; i++)
            {
                if (i != +_schema.Count - 1)
                {
                    schemaStr += _schema[i].getName() + ":" + _schema[i].getType() + "|";
                }
                else schemaStr += _schema[i].getName() + ":" + _schema[i].getType();
            }
            return schemaStr;
        }
        public Table(string name, string path, string schema)
        {
            _name = name;
            _path = path + "\\" + name;
            _schema = schemaStrToList(schema);
        }
        public static Table? join(Table table1, Table table2, string column,string nameNewTable,string databasePath) {
            if ((table1.hasColumn(column) == false) || (table2.hasColumn(column) == false)) return null;
            if(table1.getColType(column)!=table2.getColType(column)) return null;
            List<string> schemaTable1 = new List<string>(table1.getSchemaStr().Split("|"));
            List<string> schemaTable2 = new List<string>(table2.getSchemaStr().Split("|"));
            List<string> schemaNewTable = new List<string>(schemaTable1);
            int indexColTable1 = schemaTable1.FindIndex(c => c.Split(":")[0] == column);
            int indexColTable2 = schemaTable2.FindIndex(c => c.Split(":")[0] == column);
            foreach (string col in schemaTable2) {
                if (schemaTable1.Find(c => c.Split(":")[0] == col.Split(":")[0]) is null) schemaNewTable.Add(col);
            }
            string stringSchemaNewTable = schemaNewTable[0];
            for (int i = 1; i < schemaNewTable.Count; i++) {
                stringSchemaNewTable += "|"+schemaNewTable[i];
            }
            Table newTable = new Table(nameNewTable,databasePath,stringSchemaNewTable);
            newTable.create();
            List<string> rowsTable1 = table1.getAllRows();
            List<string> rowsTable2 = table2.getAllRows();
            foreach (var row in rowsTable1) {
                List<string> matchRows=rowsTable2.FindAll(c => (c.Split("|")[indexColTable2] == row.Split("|")[indexColTable1]));
                foreach (var mr in matchRows) {
                    string joinRow = row;
                    string[] splitMr = mr.Split("|");
                    for (int i = schemaTable1.Count; i < schemaNewTable.Count; i++) {
                        joinRow += "|" + splitMr[schemaTable2.FindIndex(c => c.Split(":")[0] == schemaNewTable[i].Split(":")[0])];
                    }
                    newTable.addRow(joinRow);
                }
            }
            return newTable;
        }
        public bool hasColumn(string column) {
            Column? col = _schema.Find(c => c.getName() == column);
            if (col is not null) return true;
            return false;
        }
        public string? getColType(string column) {
            return _schema.Find(c => c.getName() == column)?.getType();
        }
        public string getName() { return _name; }
        public void create()
        {
            Directory.CreateDirectory(_path);
            foreach (Column col in _schema)
            {
                col.create(_path);
            }
            string schemaPath = _path + "\\" + "schema";
            File.Create(schemaPath).Close();
            using (StreamWriter writer = new StreamWriter(schemaPath))
            {
                foreach (Column col in _schema)
                {
                    writer.WriteLine(col.getName() + ":" + col.getType());
                }
                writer.Close();
            }
        }
        public bool addRow(string arg)
        {
            string[] args = arg.Split("|");
            if (args.Length != _schema.Count) return false;
            for (int i = 0; i < _schema.Count; i++)
            {
                if (_schema[i].checkValue(args[i]) == false) return false;
            }
            for (int i = 0; i < _schema.Count; i++)
            {
                _schema[i].add(args[i]);
            }
            return true;
        }
        private List<Column> schemaStrToList(string schemaStr)
        {
            string[] columns = schemaStr.Split("|");
            List<Column> schema = new List<Column>();
            foreach (var col in columns)
            {
                string colName = col.Split(":")[0];
                string colType = col.Split(":")[1];
                schema.Add(new Column(colName, colType));
            }
            return schema;
        }
        public void delete()
        {
            Directory.Delete(_path,true);
        }
        public void open()
        {
            using (StreamReader writer = new StreamReader(_path + "\\" + "schema"))
            {
                List<Column> schema = new List<Column>();
                while (!writer.EndOfStream)
                {
                    string? line = writer.ReadLine();
                    string[] col = new string[2];
                    if (line is not null) col = line.Split(":");
                    Column column = new Column(col[0], col[1]);
                    column.setPath(_path);
                    schema.Add(column);
                }
                this._schema = schema;
                writer.Close();
            }
        }
        public string? getRow(int id)
        {
            string row = "";
            for (int i = 0; i < _schema.Count; i++)
            {
                string? value = _schema[i].getValue(id);
                if (value is null) return null;
                if (i != _schema.Count - 1)
                {
                    row += value + "|";
                }
                else row += value;
            }
            return row;
        }
        public List<string> getAllRows()
        {
            List<string> rows = new List<string>();
            for (int i = 0; ; i++)
            {
                string? row = getRow(i);
                if (row is null) return rows;
                rows.Add(row);
            }
        }
        public bool changeValue(int rowId, string columnName, string value)
        {
            Column? column = _schema.Find(c => c.getName() == columnName);
            if (column is null) return false;
            return column.changeValue(rowId, value);
        }
        public static bool compareTables(Table table1, Table table2)
        {
            if (table1._schema.Count != table2._schema.Count) return false;
            for (int i = 0; i < table1._schema.Count; i++)
            {
                if (!Column.compareColumns(table1._schema[i], table2._schema[i])) return false;
            }
            return true;
        }
    }
    class Column
    {
        private string _name;
        private string _type;
        private string _path = "";
        public string getName() { return _name; }
        public string getType() { return _type; }
        public Column(string name, string type)
        {
            _name = name;
            _type = type;
        }
        public void setPath(string path)
        {
            this._path = path + "\\" + this.getName() + "(" + this.getType() + ")";
        }
        public void create(string _path)
        {
            this._path = _path + "\\" + this.getName() + "(" + this.getType() + ")";
            File.Create(this._path).Close();
        }
        public bool add(string value)
        {
            using (StreamWriter writer = new StreamWriter(_path, true))
            {
                if (this.checkValue(value) == true)
                {
                    writer.WriteLine(value);
                    return true;
                }
                return false;
            }
        }
        public bool checkValue(string value)
        {
            if (_type == "string") return true;
            if (_type == "int")
            {
                return Int32.TryParse(value, out _);
            }
            if (_type == "real")
            {
                return Double.TryParse(value, out _);
            }
            if (_type == "char")
            {
                return Char.TryParse(value, out _);
            }
            if (_type == "$lvnl") {
                try {
                    string[] numbers=value.Split("-");
                    if (testNumberForlnvl(numbers[0])==false || testNumberForlnvl(numbers[1])==false) return false;
                    double number1 = Double.Parse(numbers[0]);
                    double number2 = Double.Parse(numbers[1]);
                    if (number1 >= number2) return false;
                    return true;
                } 
                catch {return false; }
            }
            if (_type == "$")
            {
                return testNumberForlnvl(value);
            }
            return false;
        }
        private bool testNumberForlnvl(string snumber) {
            double maxValue = 10000000000000.00;
            string[] separated = snumber.Split(",");
            if (separated.Length==2 && separated[1].Length > 2) return false;
            if(Double.TryParse(snumber,out _)==false) return false;
            double value = Double.Parse(snumber);
            if (value > maxValue) return false;
            return true;
        }
        public string? getValue(int id)
        {
            using (StreamReader reader = new StreamReader(_path))
            {
                for (int i = 0; i < id; i++)
                {
                    reader.ReadLine();
                }
                return reader.ReadLine();
            }
        }
        public bool changeValue(int rowId, string value)
        {
            if (checkValue(value) == false) return false;
            List<string> column = new List<string>();
            using (StreamReader reader = new StreamReader(_path))
            {
                string? line = reader.ReadLine();
                while (line is not null)
                {
                    column.Add(line);
                    line = reader.ReadLine();
                }
            }
            using (StreamWriter writer = new StreamWriter(_path))
            {
                column[rowId] = value;
                foreach (string columnValue in column)
                {
                    writer.WriteLine(columnValue);
                }
            }
            return true;
        }
        public static bool compareColumns(Column column1, Column column2)
        {
            if ((column1._name != column2._name) || (column1._type != column2._type)) return false;
            return true;
        }
    }
    class QueryProcessor
    {
        public Database database = new Database("D:\\", "newDatabase");
        public Table? activeTable = null;
        public void process(string querry)
        {
            string[] args = querry.Split(" ");
            if (args[0] == "create")
            {
                database = new Database(args[1], args[2]);
                database.create();
                return;
            }
            if (args[0] == "open")
            {
                database = new Database(args[1], args[2]);
                database.open();
                return;
            }
            if (args[0] == "delete") {
                activeTable = null;
                database.delete();
                database= new Database("D:\\", "newDatabase");
            }
            if (args[0] == "createTable")
            {
                database.createTable(args[1] + " " + args[2]);
                activeTable = database.getTable(args[1]);
                return;
            }
            if (args[0] == "deleteTable")
            {
                database.deleteTable(args[1]);
                if(activeTable is not null)
                    if (activeTable.getName() == args[1])
                            activeTable = null;
                return;
            }
            if (args[0] == "openTable")
            {
                var rows = database.getTable(args[1])?.getAllRows();
                if (rows is not null)
                    foreach (var row in rows)
                        Console.WriteLine(row);
                activeTable = database.getTable(args[1]);
                return;
            }
            if (args[0] == "addRow")
            {
                var rows = database.getTable(args[1]);
                int index = querry.IndexOf(args[1]);
                string arg = querry.Remove(0, index + args[1].Length + 1);
                rows?.addRow(arg);
                activeTable=database.getTable(args[1]);
                return;
            }
            if (args[0] == "union")
            {
                Table? table1 = database.getTable(args[1]);
                Table? table2 = database.getTable(args[2]);
                if ((table1 is not null) && (table2 is not null))
                    database.union(table1, table2, args[3]);
                activeTable = database.getTable(args[3]);
                return;
            }
            if (args[0] == "changeValue")
            {
                Table? table = database.getTable(args[1]);
                string columnName = args[2];
                int id = Int32.Parse(args[3]);
                int index = querry.IndexOf(args[3]);
                string arg = querry.Remove(0, index + args[3].Length + 1);
                if (table is not null) table.changeValue(id, columnName, arg);
                activeTable = database.getTable(args[1]);
                return;
            }
            if (args[0] == "join") {
                Table? table1 = database.getTable(args[1]);
                Table? table2 = database.getTable(args[2]);
                if(table1 is not null && table2 is not null)
                activeTable = database.joinTable(table1, table2, args[3],args[4]);
                return;
            }
        }
    }
}
