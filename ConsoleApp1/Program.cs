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
                Console.WriteLine(@"1. Show Records
2. Insert Record
3. Update Record
4. Delete Record
5. Reconciliação
6. Exit");
                Console.WriteLine("Enter Your Choice: ");
                ch = Int32.Parse(Console.ReadLine());

                switch (ch)
                {
                    case 1:  //Display Records
                        ob.ShowRecords();
                        break;
                    case 2:
                        Console.WriteLine("Insert a Record...");
                        //Insert a Record
                        ob.InsertRecord();
                        break;
                    case 3:  //Update Record
                        Console.WriteLine("Update Record...");
                        ob.UpdateRecord();
                        break;
                    case 4: //Delete Record
                        Console.WriteLine("Delete Record...");
                        ob.DeleteRecord();
                        break;
                    case 5:
                        Console.WriteLine("Reconcile Data...");
                        ob.reconcile();
                        break;
                    case 6:
                        Environment.Exit(0);
                        break;
                    default:
                        Console.WriteLine("Invalid Choice Entered!");
                        break;
                }
            }
        }

        // NOVO MÉTODO: Tentar conectar (usado apenas no arranque)
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

        // NOVO MÉTODO: Inicialização da aplicação
        public void InitializeApplication()
        {
            Console.WriteLine("=== INICIALIZANDO APLICAÇÃO ===");

            // Carregar instruções pendentes
            LoadInstructions();

            // Tentar conectar à base de dados
            if (TryConnect())
            {
                Console.WriteLine("✅ APLICAÇÃO INICIADA EM MODO ONLINE");
                // Carregar dados da BD
                LoadDataFromDatabase();
            }
            else
            {
                Console.WriteLine("⚠️  APLICAÇÃO INICIADA EM MODO OFFLINE");
                // Carregar dados do XML local
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
                ds = new DataSet();
                // IMPORTANTE: Incluir RowVersion na query
                da = new SqlDataAdapter("select EncId, ClienteId, Data, Total, RowVersion from Encomenda", con);
                da.Fill(ds, "Encomenda");

                // Salvar também em XML para backup
                ds.WriteXml("dataset.xml", XmlWriteMode.WriteSchema);
                Console.WriteLine("Dados carregados da base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao carregar dados da BD: {ex.Message}");
                // Fallback para XML se a BD falhar
                LoadDataFromXML();
            }
        }

        private void LoadDataFromXML()
        {
            try
            {
                ds = new DataSet();
                string filePath = "dataset.xml";

                if (File.Exists(filePath))
                {
                    ds.ReadXml(filePath, XmlReadMode.ReadSchema);
                    Console.WriteLine("Dados carregados do ficheiro XML local");
                }
                else
                {
                    // Criar DataSet vazio se não existir XML
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

        // NOVO MÉTODO: Criar DataSet vazio
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

        // MÉTODO SIMPLIFICADO: Não faz mais FetchData
        public void ShowRecords()
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

                    int x = Convert.ToInt32(dr["EncId"]);
                    int x1 = Convert.ToInt32(dr["ClienteId"]);
                    string s1 = dr["Data"].ToString();
                    string s2 = dr["Total"].ToString();
                    string str = "EncID: " + x + ", ClienteID: " + x1 + ", Data: " + s1 + ", Total: " + s2;
                    Console.WriteLine(str);
                }
                catch
                {
                    break;
                }
            }
        }

        public void InsertRecord()
        {
            Console.WriteLine($"\n=== INSERT ({(connected ? "ONLINE" : "OFFLINE")}) ===");

            try
            {
                Console.WriteLine("Insira o id do cliente que efetuou a encomenda: ");
                int id_cliente = Int32.Parse(Console.ReadLine());

                DateTime dataAtual = DateTime.Now;
                Console.WriteLine("Data atual: " + dataAtual);

                Console.WriteLine("Insira o total : ");
                decimal total = Decimal.Parse(Console.ReadLine());

                if (connected)
                {
                    Console.WriteLine("Executando INSERT na base de dados...");
                    InsertToDatabase(id_cliente, dataAtual, total);
                }
                else
                {
                    Console.WriteLine("Executando INSERT offline...");
                    InsertOffline(id_cliente, dataAtual, total);
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

        // NOVO MÉTODO: Insert na BD
        private void InsertToDatabase(int idCliente, DateTime data, decimal total)
        {
            try
            {
                SqlCommandBuilder c = new SqlCommandBuilder(da);
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

        // NOVO MÉTODO: Insert offline
        private void InsertOffline(int idCliente, DateTime data, decimal total)
        {
            // Adicionar à lista de instruções pendentes
            Instruction s = new Instruction();
            s.setInfo("insert", idCliente, total, -1);
            instructions.Add(s);
            SaveInstructions();

            // Calcular novo ID
            int novoEncId = 1;
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
                novoEncId = maxId + 1;
            }

            // Adicionar ao DataSet local
            DataRow dr = ds.Tables["Encomenda"].NewRow();
            dr["EncId"] = novoEncId;
            dr["ClienteId"] = idCliente;
            dr["Data"] = data;
            dr["Total"] = total;
            ds.Tables["Encomenda"].Rows.Add(dr);

            Console.WriteLine($"Instrução pendente criada (ID temporário: {novoEncId})");
        }

        public void UpdateRecord()
        {
            Console.WriteLine($"\n=== UPDATE ({(connected ? "ONLINE" : "OFFLINE")}) ===");

            Console.WriteLine("Insira o Id da encomenda:");
            int encId = Int32.Parse(Console.ReadLine());

            DateTime dataAtual = DateTime.Now;
            Console.WriteLine("Nova data: " + dataAtual);

            Console.WriteLine("Insira um novo total: ");
            decimal novoTotal = Decimal.Parse(Console.ReadLine());

            if (connected)
            {
                UpdateToDatabase(encId, dataAtual, novoTotal);
            }
            else
            {
                UpdateOffline(encId, dataAtual, novoTotal);
            }

            ds.WriteXml("dataset.xml", XmlWriteMode.WriteSchema);
            Console.WriteLine("✅ Registo atualizado com sucesso");
        }

        private void UpdateToDatabase(int encId, DateTime data, decimal total)
        {
            try
            {
                SqlCommandBuilder c = new SqlCommandBuilder(da);
                bool recordFound = false;

                foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
                {
                    if (dr.RowState == DataRowState.Deleted) continue;

                    // Verificar se o valor não é DBNull antes de converter
                    if (dr["EncId"] != DBNull.Value)
                    {
                        int x1 = Convert.ToInt32(dr["EncId"]);
                        if (encId == x1)
                        {
                            dr["Data"] = data;
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

        private void UpdateOffline(int encId, DateTime data, decimal total)
        {
            // Guardar informações originais para deteção de conflitos
            DataRow targetRow = null;
            foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
            {
                if (dr.RowState == DataRowState.Deleted) continue;

                // Verificar se o valor não é DBNull antes de converter
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
                Instruction s = new Instruction();
                s.setInfo("update", -1, total, encId);

                // Guardar informações originais para deteção de conflitos
                byte[] originalRowVersion = targetRow["RowVersion"] != DBNull.Value ?
                    targetRow["RowVersion"] as byte[] : null;
                DateTime originalData = targetRow["Data"] != DBNull.Value ?
                    Convert.ToDateTime(targetRow["Data"]) : DateTime.MinValue;
                s.setConflictInfo(originalRowVersion, originalData);

                instructions.Add(s);
                SaveInstructions();

                // Atualizar no DataSet local
                targetRow["Data"] = data;
                targetRow["Total"] = total;

                Console.WriteLine("Instrução de update pendente criada");
            }
            else
            {
                Console.WriteLine("Registo não encontrado");
            }
        }

        public void DeleteRecord()
        {
            Console.WriteLine($"\n=== DELETE ({(connected ? "ONLINE" : "OFFLINE")}) ===");

            Console.WriteLine("Insira o Id da encomenda que pretende eliminar:");
            int encId = Int32.Parse(Console.ReadLine());

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

        private void DeleteFromDatabase(int encId)
        {
            try
            {
                SqlCommandBuilder c = new SqlCommandBuilder(da);
                bool recordFound = false;

                foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
                {
                    if (dr.RowState == DataRowState.Deleted) continue;

                    // Verificar se o valor não é DBNull antes de converter
                    if (dr["EncId"] != DBNull.Value)
                    {
                        int x1 = Convert.ToInt32(dr["EncId"]);
                        if (encId == x1)
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

        private void DeleteOffline(int encId)
        {
            // Guardar informações originais para deteção de conflitos
            DataRow targetRow = null;
            foreach (DataRow dr in ds.Tables["Encomenda"].Rows)
            {
                if (dr.RowState == DataRowState.Deleted) continue;

                // Verificar se o valor não é DBNull antes de converter
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
                Instruction s = new Instruction();
                s.setInfo("delete", -1, -1, encId);

                // Guardar informações originais para deteção de conflitos
                byte[] originalRowVersion = targetRow["RowVersion"] != DBNull.Value ?
                    targetRow["RowVersion"] as byte[] : null;
                DateTime originalData = targetRow["Data"] != DBNull.Value ?
                    Convert.ToDateTime(targetRow["Data"]) : DateTime.MinValue;
                s.setConflictInfo(originalRowVersion, originalData);

                instructions.Add(s);
                SaveInstructions();

                // Eliminar do DataSet local
                targetRow.Delete();
                Console.WriteLine("Instrução de delete pendente criada");
            }
            else
            {
                Console.WriteLine("Registo não encontrado");
            }
        }

        public void SaveInstructions()
        {
            try
            {
                string instructionsFile = "pending_instructions.txt";
                using (StreamWriter writer = new StreamWriter(instructionsFile))
                {
                    foreach (var instruction in instructions)
                    {
                        // Formato: statement|idClient|total|idEncomenda|originalRowVersion|originalData
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
                                    int.Parse(parts[3])                // idEncomenda
                                );

                                // Carregar informações de conflito se existirem
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

        public void reconcile()
        {
            Console.WriteLine("\n=== RECONCILIAÇÃO ===");

            if (instructions.Count == 0)
            {
                Console.WriteLine("Nenhuma instrução pendente para reconciliar");
                return;
            }

            Console.WriteLine($"Total de instruções pendentes: {instructions.Count}");

            // Se já estamos online, só executar as instruções
            if (connected)
            {
                ExecutePendingInstructionsWithConflictDetection();
            }
            else
            {
                // Tentar conectar para reconciliação
                if (TryConnect())
                {
                    Console.WriteLine("Conexão estabelecida para reconciliação");
                    LoadDataFromDatabase(); // Recarregar dados frescos
                    ExecutePendingInstructionsWithConflictDetection();
                }
                else
                {
                    Console.WriteLine("❌ Não foi possível conectar para reconciliação");
                    return;
                }
            }
        }

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

            // Processar conflitos
            if (conflicts.Count > 0)
            {
                Console.WriteLine($"\n⚠️ {conflicts.Count} conflito(s) detetado(s)!");
                ProcessConflicts(conflicts, successfulInstructions);
            }
            else
            {
                // Sem conflitos - limpar todas as instruções
                FinishReconciliation(successfulInstructions);
            }
        }

        private ConflictInfo DetectUpdateConflict(Instruction instruction)
        {
            if (instruction.originalRowVersion == null) return null;

            // Verificar se o registo ainda existe e se foi modificado
            string query = "SELECT EncId, ClienteId, Data, Total, RowVersion FROM Encomenda WHERE EncId = @EncId";

            using (SqlCommand cmd = new SqlCommand(query, con))
            {
                cmd.Parameters.AddWithValue("@EncId", instruction.idEncomenda);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        reader.Close();
                        // Registo foi eliminado - conflito
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

                    // Comparar RowVersions
                    if (!ByteArraysEqual(instruction.originalRowVersion, currentRowVersion))
                    {
                        // Conflito detetado - buscar dados atuais
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

            return null; // Sem conflito
        }

        private ConflictInfo DetectDeleteConflict(Instruction instruction)
        {
            if (instruction.originalRowVersion == null) return null;

            // Verificar se o registo ainda existe e se foi modificado
            string query = "SELECT EncId, ClienteId, Data, Total, RowVersion FROM Encomenda WHERE EncId = @EncId";

            using (SqlCommand cmd = new SqlCommand(query, con))
            {
                cmd.Parameters.AddWithValue("@EncId", instruction.idEncomenda);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        reader.Close();
                        return null; // Já foi eliminado - sem conflito
                    }

                    byte[] currentRowVersion = (byte[])reader["RowVersion"];

                    // Comparar RowVersions
                    if (!ByteArraysEqual(instruction.originalRowVersion, currentRowVersion))
                    {
                        // Conflito detetado - registo foi modificado antes da tentativa de eliminação
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

            return null; // Sem conflito
        }

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
                    if (choice == 1) // Manter versão local
                    {
                        syncUpdateWithRowVersion(conflict.EncId, conflict.LocalTotal, conflict.ServerRowVersion);
                        Console.WriteLine("✅ Versão local aplicada");
                    }
                    else if (choice == 2) // Manter versão servidor
                    {
                        Console.WriteLine("✅ Versão servidor mantida (nenhuma ação necessária)");
                    }
                    // choice == 3: Ignorar (nenhuma ação)
                    break;

                case "update_deleted":
                    if (choice == 1) // Recriar registo
                    {
                        // Usar insert para recriar o registo
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
                    // choice == 2: Aceitar eliminação (nenhuma ação)
                    break;

                case "delete_modified":
                    if (choice == 1) // Eliminar mesmo assim
                    {
                        syncDeleteWithRowVersion(conflict.EncId, conflict.ServerRowVersion);
                        Console.WriteLine("✅ Registo eliminado");
                    }
                    // choice == 2: Manter registo (nenhuma ação)
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
            // Remover apenas as instruções resolvidas
            foreach (var resolved in resolvedInstructions)
            {
                instructions.RemoveAll(i => i.idEncomenda == resolved.idEncomenda &&
                                          i.statement == resolved.statement &&
                                          i.timestamp == resolved.timestamp);
            }

            if (instructions.Count == 0)
            {
                // Limpar ficheiro de instruções se todas foram resolvidas
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
                // Salvar instruções restantes
                SaveInstructions();
                Console.WriteLine($"⚠️ Reconciliação parcial - {instructions.Count} instrução(ões) ainda pendente(s)");
            }

            // Recarregar dados da BD
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