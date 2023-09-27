namespace Database
{
    public partial class Form1 : Form
    {
        private QueryProcessor processor;
        public Form1()
        {
            InitializeComponent();
            processor = new QueryProcessor();
            
        }


        private void textBox1_KeyPress(object sender, KeyPressEventArgs e)
        {

            if (e.KeyChar == (char)Keys.Enter)
            {
                processor.process(textBox1.Text);
                string command = textBox1.Text.Split(" ")[0];
                if ((command == "open") ||(command=="deleteTable")|| (command == "db")) {
                    showDatabase(processor.database);
                }
                else showTable(processor.activeTable);
                textBox1.Text = "";
            }
        }
        private void showTable(Table? table) {
            if (table is null)
            {
                dataGridView1.Columns.Clear();
            }
            else {
                dataGridView1.Columns.Clear();
                string[] schema = table.getSchemaStr().Split("|");
                foreach (string col in schema) {
                    string[] nameAndType=col.Split(":");
                    dataGridView1.Columns.Add(nameAndType[0], col);
                }
                List<string> rows = table.getAllRows();
                foreach (var row in rows) {
                    string[] a=row.Split("|");
                    dataGridView1.Rows.Add(a);
                }
            }
        }
        private void showDatabase(Database db) {
            dataGridView1.Columns.Clear();
            dataGridView1.Columns.Add("tables","tables");
            foreach (var t in db._tables) {
                dataGridView1.Rows.Add(t.getName());
            }
        }

        private void dataGridView1_CellEndEdit(object sender, DataGridViewCellEventArgs e)
        {
            string? value = dataGridView1.Rows[e.RowIndex].Cells[e.ColumnIndex].Value.ToString();
            if (value is null) value = "";
            string querry = "changeValue ";
            querry += processor.activeTable.getName() + " ";
            querry += dataGridView1.Columns[e.ColumnIndex].Name + " ";
            querry +=e.RowIndex+" ";
            querry +=value;
            processor.process(querry);
            showTable(processor.activeTable);
        }
        private void textBox1_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                e.SuppressKeyPress = true;
            }
        }
    }
}