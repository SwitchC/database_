namespace Database
{
    [TestClass]
    public class UnitTest
    {
        [TestMethod]
        public void TestJoin()
        {
            Database db = new Database("D:\\","testDB");
            db.create();
            db.createTable("table1 name:string|age:int");
            db.createTable("table2 name:string|email:string");
            Table table1 = db.getTable("table1");
            Table table2 = db.getTable("table2");
            table1.addRow("Vitalii|20");
            table2.addRow("Vitalii|qw2r5@email.com");
            Table joinTable=db.joinTable(table1, table2, "name", "joinTable");
            string expected = "Vitalii|20|qw2r5@email.com";
            string actual = joinTable.getRow(0);
            Assert.AreEqual(expected, actual);
        }
        [TestMethod]
        public void TestTypeAndAddRow() {
            string[] testRows = { "1|12,4|q|ty", "1|q|12,4|ty","ty|q|12,4|1","1|12,4|ty|q" };
            Database db = new Database("D:\\", "testDB");
            db.create();
            db.createTable("table3 i:int|r:real|c:char|s:string");
            Table table3 = db.getTable("table3");
            foreach (string testRow in testRows) {
                table3.addRow(testRow);
            }
            List<string> rows = table3.getAllRows();
            string expected = "1|12,4|q|ty";
            int expectedCount = 1;
            Assert.AreEqual (expectedCount, rows.Count);
            string actual = rows[0];
            Assert.AreEqual (expected, actual);
        }
        [TestMethod]
        public void testChangeValue() {
            Database db = new Database("D:\\", "testDB");
            db.create();
            db.createTable("table4 i:int|r:real");
            Table table4 = db.getTable("table4");
            table4.addRow("1|12,2");
            table4.changeValue(0, "r", "999123,1231");
            string expected = "1|999123,1231";
            string actual = table4.getRow(0);
            Assert.AreEqual(expected, actual);
        }
    }
}