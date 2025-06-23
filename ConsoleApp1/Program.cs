using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DatabaseExample4
{
    //Disconnected approach for database access
    class Program
    {
        // Classe para tracking de conflitos
        class ConflictInfo
        {
            public int EncId { get; set; }
            public string Operation { get; set; }
            public DataRow LocalVersion { get; set; }
            public DataRow ServerVersion { get; set; }
            public byte[] LocalRowVersion { get; set; }
            public byte[] ServerRowVersion { get; set; }
            public DateTime LocalData { get; set; }
            public decimal LocalTotal { get; set; }
            public DateTime ServerData { get; set; }
            public decimal ServerTotal { get; set; }
        }

        class Instruction
        {
            public string statement;
            public int idClient;
            public decimal total;
            public int idEncomenda;
            public DateTime timestamp;
            public byte[] originalRowVersion; // Versão original para deteção de conflitos
            public DateTime originalData; // Data original para comparação

            public void setInfo(string statement, int idClient, decimal total, int idEncomenda)
            {
                this.statement = statement;
                this.idClient = idClient;
                this.total = total;
                this.idEncomenda = idEncomenda;
                this.timestamp = DateTime.Now;
            }

            public void setConflictInfo(byte[] rowVersion, DateTime originalData)
            {
                this.originalRowVersion = rowVersion;
                this.originalData = originalData;
            }
        }

        List<Instruction> instructions = new List<Instruction>();
        SqlConnection con;
        SqlDataAdapter da;
        DataSet ds;
        int n;
        Boolean connected = false;

        static void Main(string[] args)
        {
            Program ob = new Program();
            ob.InitializeApplication();

            int ch;
            while (true)
            {
                Console.WriteLine("Select a Database Operation: ");
                Console.WriteLine(@"1. Mostrar Encomendas
                                    2. Inserir Encomendas
                                    3. Atualizar Encomenda
                                    4. Eliminar Encomenda
                                    5. Reconciliar com Servidor BD
                                    6. sair");
                Console.WriteLine("Enter Your Choice: ");
                ch = Int32.Parse(Console.ReadLine());

                switch (ch)
                {
                    case 1:  //Display Records
                        ob.ShowOrders();
                        break;
                    case 2:
                        Console.WriteLine("Inserir uma Encomenda");
                        //Insert a Record
                        ob.InsertOrder();
                        break;
                    case 3:  //Update Record
                        Console.WriteLine("Atualizar uma Encomenda");
                        ob.UpdateOrder();
                        break;
                    case 4: //Delete Record
                        Console.WriteLine("Eliminar uma Encomenda");
                        ob.DeleteOrder();
                        break;
                    case 5:
                        Console.WriteLine("Reconciliar Dados com Servidor BD");
                        ob.reconcile();
                        break;
                    case 6:
                        Environment.Exit(0);
                        break;
                    default:
                        Console.WriteLine("Escolha Inválida");
                        break;
                }
            }
        }

        // Used only while starting the app - used to check if connection to SQL is possible or not
        private bool TryConnect()
        {
            try
            {
                string connectionString = @"Server=localhost\SQLEXPRESS;Database=Trab2_6;Integrated Security=True;Encrypt=True;TrustServerCertificate=True";
                con = new SqlConnection(connectionString);
                con.Open();
                connected = true;
                Console.WriteLine("Conexão à base de dados estabelecida");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Falha na conexão: {ex.Message}");
                connected = false;
                return false;
            }
        }

        // App Initialization
        public void InitializeApplication()
        {
            Console.WriteLine("=== INICIALIZANDO APLICAÇÃO ===");

            // Loads Pending Instructions
            LoadInstructions();

            if (TryConnect())
            {
                Console.WriteLine("✅ APLICAÇÃO INICIADA EM MODO ONLINE");
                // If connection is up, load data from database
                LoadDataFromDatabase();
            }
            else
            {
                Console.WriteLine("⚠️  APLICAÇÃO INICIADA EM MODO OFFLINE");
                // If connection is down, load data from XML file
                LoadDataFromXML();
            }

            if (instructions.Count > 0)
            {
                Console.WriteLine($"📋 {instructions.Count} instrução(ões) pendente(s) encontrada(s)");
            }

            Console.WriteLine("=== INICIALIZAÇÃO CONCLUÍDA ===\n");
        }

        private void LoadDataFromDatabase()
        {
            try
            {
                // Create new DataSet and fill it with Encomenda tabel
                ds = new DataSet();
                da = new SqlDataAdapter("select EncId, ClienteId, Data, Total, RowVersion from Encomenda", con);
                da.Fill(ds, "Encomenda");

                // Write XML as well, for backup
                ds.WriteXml("dataset.xml", XmlWriteMode.WriteSchema);
                Console.WriteLine("Dados carregados da base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao carregar dados da BD: {ex.Message}");
                LoadDataFromXML();
            }
        }

        private void LoadDataFromXML()
        {
            try
            {
 
                ds = new DataSet();
                string filePath = "dataset.xml";

                // If there's an existing XML file, fill dataset with it's info
                if (File.Exists(filePath))
                {
                    ds.ReadXml(filePath, XmlReadMode.ReadSchema);
                    Console.WriteLine("Dados carregados do ficheiro XML local");
                }
                // If there's no XML file, create a empty DataSet
                else
                {
                    CreateEmptyDataSet();
                    Console.WriteLine("Criado dataset vazio (sem ficheiro XML encontrado)");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao carregar XML: {ex.Message}");
                CreateEmptyDataSet();
            }
        }

        // Creating empty DataSet
        private void CreateEmptyDataSet()
        {
            ds = new DataSet();
            DataTable table = new DataTable("Encomenda");
            table.Columns.Add("EncId", typeof(int));
            table.Columns.Add("ClienteId", typeof(int));
            table.Columns.Add("Data", typeof(DateTime));
            table.Columns.Add("Total", typeof(decimal));
            table.Columns.Add("RowVersion", typeof(byte[]));
            ds.Tables.Add(table);
        }

        // Shows existing orders
        public void ShowOrders()
        {
            Console.WriteLine($"\n=== REGISTOS ({(connected ? "ONLINE" : "OFFLINE")}) ===");

            if (ds.Tables["Encomenda"].Rows.Count == 0)
            {
                Console.WriteLine("Nenhum registo encontrado.");
                return;
            }

            foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
            {
                try
                {
                    if (dr.RowState == DataRowState.Deleted) continue;

                    int encId = Convert.ToInt32(dr["EncId"]);
                    int clientId = Convert.ToInt32(dr["ClienteId"]);
                    string date = dr["Data"].ToString();
                    string total = dr["Total"].ToString();
                    string str = "EncID: " + encId + ", ClienteID: " + clientId + ", Data: " + date + ", Total: " + total;
                    Console.WriteLine(str);
                }
                catch
                {
                    break;
                }
            }
        }

        //Inserts Order
        public void InsertOrder()
        {
            Console.WriteLine($"\n=== INSERT ({(connected ? "ONLINE" : "OFFLINE")}) ===");

            try
            {
                Console.WriteLine("Insira o id do cliente que efetuou a encomenda: ");
                int clientId = Int32.Parse(Console.ReadLine());

                DateTime currentDate = DateTime.Now;
                Console.WriteLine("Data atual: " + currentDate);

                Console.WriteLine("Insira o total : ");
                decimal total = Decimal.Parse(Console.ReadLine());

                // Sends method to other method, based in being connected or not 
                if (connected)
                {
                    Console.WriteLine("Executando INSERT na base de dados...");
                    InsertToDatabase(clientId, currentDate, total);
                }
                else
                {
                    Console.WriteLine("Executando INSERT offline...");
                    InsertOffline(clientId, currentDate, total);
                }

                // Salvar XML sempre
                ds.WriteXml("dataset.xml", XmlWriteMode.WriteSchema);
                Console.WriteLine("✅ Registo inserido com sucesso");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ ERRO: {ex.Message}");
            }
        }

        // If connected, inserts into data base
        private void InsertToDatabase(int idCliente, DateTime data, decimal total)
        {
            try
            {
                SqlCommandBuilder cb = new SqlCommandBuilder(da);
                DataRow dr = ds.Tables["Encomenda"].NewRow();
                dr["ClienteId"] = idCliente;
                dr["Data"] = data;
                dr["Total"] = total;
                ds.Tables["Encomenda"].Rows.Add(dr);

                da.Update(ds, "Encomenda");
                Console.WriteLine("Dados sincronizados com a base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao inserir na BD: {ex.Message}");
                throw;
            }
        }

        // If not connected, inserts in local DataSet
        private void InsertOffline(int idCliente, DateTime data, decimal total)
        {
            // Adds insert into pending instructions
            Instruction inst = new Instruction();
            inst.setInfo("insert", idCliente, total, -1);
            instructions.Add(inst);
            SaveInstructions();

            // Calculates new ID
            int newEncId = 1;
            if (ds.Tables["Encomenda"].Rows.Count > 0)
            {
                int maxId = 0;
                foreach (DataRow row in ds.Tables["Encomenda"].Rows)
                {
                    if (row.RowState != DataRowState.Deleted && row["EncId"] != DBNull.Value)
                    {
                        int currentId = Convert.ToInt32(row["EncId"]);
                        if (currentId > maxId)
                            maxId = currentId;
                    }
                }
                newEncId = maxId + 1;
            }

            // Inserts into DataSet
            DataRow dr = ds.Tables["Encomenda"].NewRow();
            dr["EncId"] = newEncId;
            dr["ClienteId"] = idCliente;
            dr["Data"] = data;
            dr["Total"] = total;
            ds.Tables["Encomenda"].Rows.Add(dr);

            Console.WriteLine($"Instrução pendente criada (ID temporário: {newEncId})");
        }


        // Updating Orders
        public void UpdateOrder()
        {
            Console.WriteLine($"\n=== UPDATE ({(connected ? "ONLINE" : "OFFLINE")}) ===");

            Console.WriteLine("Insira o Id da encomenda:");
            int encId = Int32.Parse(Console.ReadLine());

            DateTime currentDate = DateTime.Now;
            Console.WriteLine("Nova data: " + currentDate);

            Console.WriteLine("Insira um novo total: ");
            decimal newTotal = Decimal.Parse(Console.ReadLine());

            // Sends to another method, based on if it is connected or not
            if (connected)
            {
                UpdateToDatabase(encId, currentDate, newTotal);
            }
            else
            {
                UpdateOffline(encId, currentDate, newTotal);
            }

            ds.WriteXml("dataset.xml", XmlWriteMode.WriteSchema);
            Console.WriteLine("✅ Registo atualizado com sucesso");
        }

        //If connected updated in Data Base
        private void UpdateToDatabase(int encId, DateTime date, decimal total)
        {
            try
            {
                SqlCommandBuilder c = new SqlCommandBuilder(da);
                bool recordFound = false;

                foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
                {
                    if (dr.RowState == DataRowState.Deleted) continue;

                    // Before converting, checks if value is DBNull
                    if (dr["EncId"] != DBNull.Value)
                    {
                        int encIdS = Convert.ToInt32(dr["EncId"]);
                        if (encId == encIdS)
                        {
                            dr["Data"] = date;
                            dr["Total"] = total;
                            recordFound = true;
                            break;
                        }
                    }
                }

                if (!recordFound)
                {
                    throw new Exception($"Registo com ID {encId} não encontrado");
                }

                da.Update(ds, "Encomenda");
                Console.WriteLine("Dados sincronizados com a base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao atualizar na BD: {ex.Message}");
                throw;
            }
        }

        //If not connected updates in local DataSet
        private void UpdateOffline(int encId, DateTime date, decimal total)
        {
            // Saves original data for conflict detection
            DataRow targetRow = null;
            foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
            {
                if (dr.RowState == DataRowState.Deleted) continue;

                // Check if value is not DBNull before converting
                if (dr["EncId"] != DBNull.Value)
                {
                    int x1 = Convert.ToInt32(dr["EncId"]);
                    if (encId == x1)
                    {
                        targetRow = dr;
                        break;
                    }
                }
            }

            if (targetRow != null)
            {
                Instruction inst = new Instruction();
                inst.setInfo("update", -1, total, encId);

                // Saves original data for conflict detection
                byte[] originalRowVersion = targetRow["RowVersion"] != DBNull.Value ?
                    targetRow["RowVersion"] as byte[] : null;
                DateTime originalData = targetRow["Data"] != DBNull.Value ?
                    Convert.ToDateTime(targetRow["Data"]) : DateTime.MinValue;
                inst.setConflictInfo(originalRowVersion, originalData);

                instructions.Add(inst);
                SaveInstructions();

                // Saves in local DataSet
                targetRow["Data"] = date;
                targetRow["Total"] = total;

                Console.WriteLine("Instrução de update pendente criada");
            }
            else
            {
                Console.WriteLine("Registo não encontrado");
            }
        }

        //Deleting Orders
        public void DeleteOrder()
        {
            Console.WriteLine($"\n=== DELETE ({(connected ? "ONLINE" : "OFFLINE")}) ===");

            Console.WriteLine("Insira o Id da encomenda que pretende eliminar:");
            int encId = Int32.Parse(Console.ReadLine());

            // Sends to another method, based on if it is connected or not
            if (connected)
            {
                DeleteFromDatabase(encId);
            }
            else
            {
                DeleteOffline(encId);
            }

            ds.WriteXml("dataset.xml", XmlWriteMode.WriteSchema);
            Console.WriteLine("✅ Registo eliminado com sucesso");
        }

        //If connection is up, delete from database
        private void DeleteFromDatabase(int encId)
        {
            try
            {
                SqlCommandBuilder c = new SqlCommandBuilder(da);
                bool recordFound = false;

                foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
                {
                    if (dr.RowState == DataRowState.Deleted) continue;

                    // Check if value is DBNull before converting
                    if (dr["EncId"] != DBNull.Value)
                    {
                        int encIdS = Convert.ToInt32(dr["EncId"]);
                        if (encId == encIdS)
                        {
                            dr.Delete();
                            recordFound = true;
                            break;
                        }
                    }
                }

                if (!recordFound)
                {
                    throw new Exception($"Registo com ID {encId} não encontrado");
                }

                da.Update(ds, "Encomenda");
                Console.WriteLine("Registo eliminado da base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao eliminar da BD: {ex.Message}");
                throw;
            }
        }

        //If connection is down, delete in local dataset
        private void DeleteOffline(int encId)
        {

            DataRow targetRow = null;
            foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
            {
                if (dr.RowState == DataRowState.Deleted) continue;

                // Checks if value is DBNull before converting
                if (dr["EncId"] != DBNull.Value)
                {
                    int encIdS = Convert.ToInt32(dr["EncId"]);
                    if (encId == encIdS)
                    {
                        targetRow = dr;
                        break;
                    }
                }
            }

            if (targetRow != null)
            {
                Instruction inst = new Instruction();
                inst.setInfo("delete", -1, -1, encId);

                // Saves original info for conflict detection
                byte[] originalRowVersion = targetRow["RowVersion"] != DBNull.Value ?
                    targetRow["RowVersion"] as byte[] : null;
                DateTime originalData = targetRow["Data"] != DBNull.Value ?
                    Convert.ToDateTime(targetRow["Data"]) : DateTime.MinValue;
                inst.setConflictInfo(originalRowVersion, originalData);

                instructions.Add(inst);
                SaveInstructions();

                // Deletes from local DataSet
                targetRow.Delete();
                Console.WriteLine("Instrução de delete pendente criada");
            }
            else
            {
                Console.WriteLine("Registo não encontrado");
            }
        }

        // Called after every DataSet change, to save it in the instruction file
        public void SaveInstructions()
        {
            try
            {
                string instructionsFile = "pending_instructions.txt";
                using (StreamWriter writer = new StreamWriter(instructionsFile))
                {
                    foreach (var instruction in instructions)
                    {
                        // Format: statement|idClient|total|idEnc|originalRowVersion|originalDate
                        string rowVersionStr = instruction.originalRowVersion != null ?
                            Convert.ToBase64String(instruction.originalRowVersion) : "";
                        string originalDataStr = instruction.originalData != DateTime.MinValue ?
                            instruction.originalData.ToString("yyyy-MM-dd HH:mm:ss") : "";

                        string line = $"{instruction.statement}|{instruction.idClient}|{instruction.total}|{instruction.idEncomenda}|{rowVersionStr}|{originalDataStr}";
                        writer.WriteLine(line);
                    }
                }
                Console.WriteLine($"Instruções salvas: {instructions.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao salvar instruções: {ex.Message}");
            }
        }

        // Method used in app initialization
        public void LoadInstructions()
        {
            try
            {
                string instructionsFile = "pending_instructions.txt";
                if (File.Exists(instructionsFile))
                {
                    instructions.Clear();
                    string[] lines = File.ReadAllLines(instructionsFile);

                    foreach (string line in lines)
                    {
                        if (!string.IsNullOrEmpty(line))
                        {
                            string[] parts = line.Split('|');
                            if (parts.Length >= 4)
                            {
                                Instruction instr = new Instruction();
                                instr.setInfo(
                                    parts[0],                           // statement
                                    int.Parse(parts[1]),               // idClient
                                    decimal.Parse(parts[2]),           // total
                                    int.Parse(parts[3])                // idEnc
                                );

                                // Load Conflict Infos if it exists
                                if (parts.Length >= 6)
                                {
                                    byte[] rowVersion = !string.IsNullOrEmpty(parts[4]) ?
                                        Convert.FromBase64String(parts[4]) : null;
                                    DateTime originalData = !string.IsNullOrEmpty(parts[5]) ?
                                        DateTime.ParseExact(parts[5], "yyyy-MM-dd HH:mm:ss", null) : DateTime.MinValue;

                                    instr.setConflictInfo(rowVersion, originalData);
                                }

                                instructions.Add(instr);
                            }
                        }
                    }
                    Console.WriteLine($"Instruções carregadas: {instructions.Count}");
                }
                else
                {
                    Console.WriteLine("Nenhum ficheiro de instruções encontrado");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao carregar instruções: {ex.Message}");
            }
        }

        //Reconcile Data Method
        public void reconcile()
        {
            Console.WriteLine("\n=== RECONCILIAÇÃO ===");

            if (instructions.Count == 0)
            {
                Console.WriteLine("Nenhuma instrução pendente para reconciliar");
                return;
            }

            Console.WriteLine($"Total de instruções pendentes: {instructions.Count}");

            // If connection is up, execute instructions
            if (connected)
            {
                ExecutePendingInstructionsWithConflictDetection();
            }
            else
            {
                // Try to connect for reconciling data
                if (TryConnect())
                {
                    Console.WriteLine("Conexão estabelecida para reconciliação");
                    LoadDataFromDatabase(); // Load fresh data
                    ExecutePendingInstructionsWithConflictDetection();
                }
                else
                {
                    Console.WriteLine("❌ Não foi possível conectar para reconciliação");
                    return;
                }
            }
        }

        //Executes instructions and detects conflicts. Also calls methods for each type of conflict
        private void ExecutePendingInstructionsWithConflictDetection()
        {
            List<ConflictInfo> conflicts = new List<ConflictInfo>();
            List<Instruction> successfulInstructions = new List<Instruction>();

            foreach (var item in instructions)
            {
                Console.WriteLine($"Processando: {item.statement} (ID: {item.idEncomenda})");

                try
                {
                    if (item.statement == "insert")
                    {
                        syncInsert(item.idClient, item.total);
                        successfulInstructions.Add(item);
                    }
                    else if (item.statement == "update")
                    {
                        ConflictInfo conflict = DetectUpdateConflict(item);
                        if (conflict != null)
                        {
                            conflicts.Add(conflict);
                        }
                        else
                        {
                            syncUpdate(item.idEncomenda, item.total);
                            successfulInstructions.Add(item);
                        }
                    }
                    else if (item.statement == "delete")
                    {
                        ConflictInfo conflict = DetectDeleteConflict(item);
                        if (conflict != null)
                        {
                            conflicts.Add(conflict);
                        }
                        else
                        {
                            syncDelete(item.idEncomenda);
                            successfulInstructions.Add(item);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Erro: {ex.Message}");
                }
            }

            // Conflicts Processing
            if (conflicts.Count > 0)
            {
                Console.WriteLine($"\n⚠️ {conflicts.Count} conflito(s) detetado(s)!");
                ProcessConflicts(conflicts, successfulInstructions);
            }
            else
            {
                // If there are no conflict - clear all instructions
                FinishReconciliation(successfulInstructions);
            }
        }

        //Verifies Conflicts with updated data
        private ConflictInfo DetectUpdateConflict(Instruction instruction)
        {
            if (instruction.originalRowVersion == null) return null;

            // Verifies if record still exists and if it was modified
            string query = "SELECT EncId, ClienteId, Data, Total, RowVersion FROM Encomenda WHERE EncId = @EncId";

            using (SqlCommand cmd = new SqlCommand(query, con))
            {
                cmd.Parameters.AddWithValue("@EncId", instruction.idEncomenda);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        reader.Close();
                        // Record was deleted - conflict
                        return new ConflictInfo
                        {
                            EncId = instruction.idEncomenda,
                            Operation = "update_deleted",
                            LocalTotal = instruction.total,
                            LocalData = DateTime.Now
                        };
                    }

                    byte[] currentRowVersion = (byte[])reader["RowVersion"];
                    reader.Close();

                    // Comparing RowVerions
                    if (!ByteArraysEqual(instruction.originalRowVersion, currentRowVersion))
                    {
                        // Record was modified - conflict
                        using (SqlCommand cmd2 = new SqlCommand(query, con))
                        {
                            cmd2.Parameters.AddWithValue("@EncId", instruction.idEncomenda);
                            using (SqlDataReader reader2 = cmd2.ExecuteReader())
                            {
                                if (reader2.Read())
                                {
                                    return new ConflictInfo
                                    {
                                        EncId = instruction.idEncomenda,
                                        Operation = "update",
                                        LocalTotal = instruction.total,
                                        LocalData = DateTime.Now,
                                        ServerTotal = Convert.ToDecimal(reader2["Total"]),
                                        ServerData = Convert.ToDateTime(reader2["Data"]),
                                        LocalRowVersion = instruction.originalRowVersion,
                                        ServerRowVersion = (byte[])reader2["RowVersion"]
                                    };
                                }
                            }
                        }
                    }
                }
            }

            return null; // No conflict
        }

        //Verifies Conflicts with deleted data
        private ConflictInfo DetectDeleteConflict(Instruction instruction)
        {
            if (instruction.originalRowVersion == null) return null;

            // Verifies if record still exists and if it was modified
            string query = "SELECT EncId, ClienteId, Data, Total, RowVersion FROM Encomenda WHERE EncId = @EncId";

            using (SqlCommand cmd = new SqlCommand(query, con))
            {
                cmd.Parameters.AddWithValue("@EncId", instruction.idEncomenda);

                // Verifies if record still exists - if not, no conflict
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        reader.Close();
                        return null;
                    }

                    byte[] currentRowVersion = (byte[])reader["RowVersion"];

                    // Compares RowVersions, if they are different, record was modified - conflict
                    if (!ByteArraysEqual(instruction.originalRowVersion, currentRowVersion))
                    {
                        //
                        return new ConflictInfo
                        {
                            EncId = instruction.idEncomenda,
                            Operation = "delete_modified",
                            ServerTotal = Convert.ToDecimal(reader["Total"]),
                            ServerData = Convert.ToDateTime(reader["Data"]),
                            LocalRowVersion = instruction.originalRowVersion,
                            ServerRowVersion = currentRowVersion
                        };
                    }
                }
            }

            return null; // No conflict
        }

        //Shows conflicts to user and asks for action
        private void ProcessConflicts(List<ConflictInfo> conflicts, List<Instruction> successfulInstructions)
        {
            List<Instruction> resolvedInstructions = new List<Instruction>(successfulInstructions);

            foreach (var conflict in conflicts)
            {
                Console.WriteLine($"\n=== CONFLITO DETETADO ===");
                Console.WriteLine($"Registo ID: {conflict.EncId}");
                Console.WriteLine($"Operação: {conflict.Operation}");

                if (conflict.Operation == "update")
                {
                    Console.WriteLine("\nVERSÃO LOCAL (suas alterações):");
                    Console.WriteLine($"Data: {conflict.LocalData:yyyy-MM-dd HH:mm:ss}");
                    Console.WriteLine($"Total: {conflict.LocalTotal:C}");

                    Console.WriteLine("\nVERSÃO SERVIDOR (alterações externas):");
                    Console.WriteLine($"Data: {conflict.ServerData:yyyy-MM-dd HH:mm:ss}");
                    Console.WriteLine($"Total: {conflict.ServerTotal:C}");

                    Console.WriteLine("\nEscolha uma opção:");
                    Console.WriteLine("1. Manter versão LOCAL (suas alterações)");
                    Console.WriteLine("2. Manter versão SERVIDOR (alterações externas)");
                    Console.WriteLine("3. Ignorar alteração (cancelar)");
                }
                else if (conflict.Operation == "update_deleted")
                {
                    Console.WriteLine("O registo que tentou atualizar foi eliminado por outro utilizador.");
                    Console.WriteLine("\nEscolha uma opção:");
                    Console.WriteLine("1. Recriar registo com suas alterações");
                    Console.WriteLine("2. Ignorar alteração (aceitar eliminação)");
                }
                else if (conflict.Operation == "delete_modified")
                {
                    Console.WriteLine("O registo que tentou eliminar foi modificado por outro utilizador.");
                    Console.WriteLine("\nVERSÃO ATUAL NO SERVIDOR:");
                    Console.WriteLine($"Data: {conflict.ServerData:yyyy-MM-dd HH:mm:ss}");
                    Console.WriteLine($"Total: {conflict.ServerTotal:C}");

                    Console.WriteLine("\nEscolha uma opção:");
                    Console.WriteLine("1. Eliminar mesmo assim");
                    Console.WriteLine("2. Manter registo (cancelar eliminação)");
                }

                Console.Write("Sua escolha: ");
                int choice = Int32.Parse(Console.ReadLine());

                try
                {
                    ProcessConflictResolution(conflict, choice);

                    // Encontrar e marcar a instrução como resolvida
                    var resolvedInstruction = instructions.FirstOrDefault(i =>
                        i.idEncomenda == conflict.EncId && i.statement == conflict.Operation.Split('_')[0]);
                    if (resolvedInstruction != null)
                    {
                        resolvedInstructions.Add(resolvedInstruction);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Erro ao resolver conflito: {ex.Message}");
                }
            }

            FinishReconciliation(resolvedInstructions);
        }

        private void ProcessConflictResolution(ConflictInfo conflict, int choice)
        {
            switch (conflict.Operation)
            {
                case "update":
                    if (choice == 1) //Keeping local version
                    {
                        syncUpdateWithRowVersion(conflict.EncId, conflict.LocalTotal, conflict.ServerRowVersion);
                        Console.WriteLine("✅ Versão local aplicada");
                    }
                    else if (choice == 2) // Keep server version
                    {
                        Console.WriteLine("✅ Versão servidor mantida (nenhuma ação necessária)");
                    }
                    // choice == 3: Ignorar (nenhuma ação)
                    break;

                case "update_deleted":
                    if (choice == 1) // Recreates Record
                    {
                        string insertQuery = @"INSERT INTO Encomenda (EncId, ClienteId, Data, Total) VALUES (@EncId, @ClienteId, @Data, @Total)";
                        using (SqlCommand cmd = new SqlCommand(insertQuery, con))
                        {
                            cmd.Parameters.AddWithValue("@EncId", conflict.EncId);
                            cmd.Parameters.AddWithValue("@ClienteId", 1); // Valor padrão - ajustar conforme necessário
                            cmd.Parameters.AddWithValue("@Data", conflict.LocalData);
                            cmd.Parameters.AddWithValue("@Total", conflict.LocalTotal);
                            cmd.ExecuteNonQuery();
                        }
                        Console.WriteLine("✅ Registo recriado com suas alterações");
                    }
                    // choice == 2: Accept delete
                    break;

                case "delete_modified":
                    if (choice == 1) // Delete anyway
                    {
                        syncDeleteWithRowVersion(conflict.EncId, conflict.ServerRowVersion);
                        Console.WriteLine("✅ Registo eliminado");
                    }
                    // choice == 2: Keep Record
                    break;
            }
        }

        private void syncUpdateWithRowVersion(int encId, decimal total, byte[] expectedRowVersion)
        {
            string updateQuery = @"UPDATE Encomenda SET Data = @Data, Total = @Total 
                                 WHERE EncId = @EncId AND RowVersion = @RowVersion";

            using (SqlCommand cmd = new SqlCommand(updateQuery, con))
            {
                cmd.Parameters.AddWithValue("@Data", DateTime.Now);
                cmd.Parameters.AddWithValue("@Total", total);
                cmd.Parameters.AddWithValue("@EncId", encId);
                cmd.Parameters.AddWithValue("@RowVersion", expectedRowVersion);

                int rowsAffected = cmd.ExecuteNonQuery();
                if (rowsAffected == 0)
                {
                    throw new Exception("Falha ao atualizar - registo foi modificado novamente");
                }
            }
        }

        private void syncDeleteWithRowVersion(int encId, byte[] expectedRowVersion)
        {
            string deleteQuery = @"DELETE FROM Encomenda WHERE EncId = @EncId AND RowVersion = @RowVersion";

            using (SqlCommand cmd = new SqlCommand(deleteQuery, con))
            {
                cmd.Parameters.AddWithValue("@EncId", encId);
                cmd.Parameters.AddWithValue("@RowVersion", expectedRowVersion);

                int rowsAffected = cmd.ExecuteNonQuery();
                if (rowsAffected == 0)
                {
                    throw new Exception("Falha ao eliminar - registo foi modificado novamente");
                }
            }
        }

        private bool ByteArraysEqual(byte[] array1, byte[] array2)
        {
            if (array1 == null && array2 == null) return true;
            if (array1 == null || array2 == null) return false;
            if (array1.Length != array2.Length) return false;

            for (int i = 0; i < array1.Length; i++)
            {
                if (array1[i] != array2[i]) return false;
            }
            return true;
        }

        private void FinishReconciliation(List<Instruction> resolvedInstructions)
        {
            // Remove only solved instructions
            foreach (var resolved in resolvedInstructions)
            {
                instructions.RemoveAll(i => i.idEncomenda == resolved.idEncomenda &&
                                          i.statement == resolved.statement &&
                                          i.timestamp == resolved.timestamp);
            }

            if (instructions.Count == 0)
            {
                // If all are soved, clear file
                try
                {
                    string instructionsFile = "pending_instructions.txt";
                    if (File.Exists(instructionsFile))
                    {
                        File.Delete(instructionsFile);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao limpar ficheiro: {ex.Message}");
                }

                Console.WriteLine("✅ Reconciliação concluída com sucesso - todas as instruções foram processadas");
            }
            else
            {
                SaveInstructions();
                Console.WriteLine($"⚠️ Reconciliação parcial - {instructions.Count} instrução(ões) ainda pendente(s)");
            }

            LoadDataFromDatabase();
        }

        private void syncInsert(int idCliente, decimal total)
        {
            try
            {
                string insertQuery = @"INSERT INTO Encomenda (ClienteId, Data, Total) VALUES (@ClienteId, @Data, @Total)";

                using (SqlCommand cmd = new SqlCommand(insertQuery, con))
                {
                    cmd.Parameters.AddWithValue("@ClienteId", idCliente);
                    cmd.Parameters.AddWithValue("@Data", DateTime.Now);
                    cmd.Parameters.AddWithValue("@Total", total);

                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine("✅ Insert sincronizado com a base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro no syncInsert: {ex.Message}");
                throw;
            }
        }

        private void syncUpdate(int encId, decimal total)
        {
            try
            {
                string updateQuery = @"UPDATE Encomenda SET Data = @Data, Total = @Total WHERE EncId = @EncId";

                using (SqlCommand cmd = new SqlCommand(updateQuery, con))
                {
                    cmd.Parameters.AddWithValue("@Data", DateTime.Now);
                    cmd.Parameters.AddWithValue("@Total", total);
                    cmd.Parameters.AddWithValue("@EncId", encId);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected == 0)
                    {
                        throw new Exception($"Nenhum registo encontrado com ID {encId}");
                    }
                }

                Console.WriteLine("✅ Update sincronizado com a base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro no syncUpdate: {ex.Message}");
                throw;
            }
        }

        private void syncDelete(int encId)
        {
            try
            {
                string deleteQuery = @"DELETE FROM Encomenda WHERE EncId = @EncId";

                using (SqlCommand cmd = new SqlCommand(deleteQuery, con))
                {
                    cmd.Parameters.AddWithValue("@EncId", encId);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected == 0)
                    {
                        throw new Exception($"Nenhum registo encontrado com ID {encId}");
                    }
                }

                Console.WriteLine("✅ Delete sincronizado com a base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro no syncDelete: {ex.Message}");
                throw;
            }
        }
    }
}