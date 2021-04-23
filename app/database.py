import MySQLdb
import pandas as pd

# Constantes
HOST = "localhost"
USER = "root"
PASSWORD = ""
PORT = 3306

class DataBase:

    def create_connection_and_cursor(self, db_name: str = "") -> None:
        """
        Cria a conexão com o banco de dados (MySQL) e o cursor.
        :param db_name: str, nome do banco de dados.
        :return: None
        """
        self.conn = MySQLdb.connect(host=HOST, user=USER, password=PASSWORD, port=PORT, db=db_name)
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    def conn_and_cursor_exist(self) -> bool:
        """
        Avalia se a conexão e o cursor foram criados.
        Método a ser chamado dentro das outras funções.
        :return: bool
        """
        try:
            self.conn
            self.cursor
            return True
        except AttributeError:
            return False

    def is_database_selected(self) -> bool:
        try:
            self.cursor.execute("CREATE TABLE temp_table (teste varchar(1))")
            self.cursor.execute("DROP TABLE temp_table")
            return True
        except Exception:
            return False

    def change_current_database(self, new_database_name: str) -> None:
        """
        Muda a tabela para
        :param new_database_name:  str, nome da tabela no db.
        :return: None
        """
        self.conn.select_db(new_database_name)

    def convert_list_to_sql_string(self, data: list) -> str:
        converted_to_sql_data = [f"'{value}'"
                                 if isinstance(value, str) and value.upper() != "DEFAULT" and value.upper() != "NULL"
                                 else str(value)
                                 for value in data]
        string_values = ",".join(converted_to_sql_data)
        return string_values

    def convert_dict_to_sql_string(self, data: dict, separator=",") -> str:
        """
        Método utilizado no crud_update. Recebe dicionário e retonra parte
        da string do MySQL.
        :param data: dict com keys (colunas do db) e values (valores a
        serem atualizados)
        :return: string adaptada para a query do MySQL
        """
        converted_to_sql_data = []
        for key, value in data.items():
            if isinstance(value, str) and value.upper() != "DEFAULT" and value.upper() != "NULL":
                converted_to_sql_data.append(f"{key} = '{value}'")
            else:
                converted_to_sql_data.append(f"{key} = {value}")
        string_values = f"{separator}".join(converted_to_sql_data)
        return string_values


    def insert_data(self, table_to_insert: str, data: list) -> bool:
        """

        :param table_to_insert: str, nome da tabela no db.
        :param data: ####MUDAR ISSO AQUI PARA DICT <----------------
        :return:
        """
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if not isinstance(data, list):
            raise TypeError("Data is not a list!")

        string_values = self.convert_list_to_sql_string(data)
        sql = f"""INSERT INTO {table_to_insert} VALUES ({string_values})"""

        try:
            affected_rows = self.cursor.execute(sql)
            if affected_rows > 0:
                return True
        except:
            return False
        return False

    def crud_read(self, table_to_read: str) -> bool:
        """
        Avalia se a conexão está definida, se o db está selecionado,
        se a tabela existe. Seleciona todos os elementos da tabela, carrega
         o nome das colunas em 'lista_nomes_colunas' e printa os dados como
         DataFrame.
        :param table_to_read: str, nome da tabela no db.
        :return: bool
        """
        if not self.conn_and_cursor_exist():
            raise Exception("Connetion or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if self.cursor.execute(f"show tables like '{table_to_read}'") != 1:
            raise Exception("Table name doesn't exist!")

        self.cursor.execute(f"select * from {table_to_read}")
        lista_nomes_colunas = []
        for col in self.cursor.description:
            lista_nomes_colunas.append(col[0])
        print(pd.DataFrame(self.cursor.fetchall(), columns=lista_nomes_colunas))
        return True



    def crud_update(self, table_to_update: str, id: int, dict_values: dict):
        """
        Avalia se a conexão está definida, se o db está selecionado,
        se a tabela existe. Encontra o nome da coluna pk, avalia se o id passado
        existe na coluna pk. Converte o dicionário com os valores de update na
        query de update e executa.
        :param table_to_update: str, nome da tabela no db.
        :param id: str, primary key.
        :param dict_values: dict com keys (colunas do db) e values (valores a
        serem atualizados).
        :return: bool
        """
        if not self.conn_and_cursor_exist():
            raise Exception("Connection or cursor is not defined!")
        if not self.is_database_selected():
            raise Exception("Database is not selected!")
        if self.cursor.execute(f"show tables like '{table_to_update}'") != 1:
            raise Exception(f"Table: {table_to_update} not found!")

        # Encontra o nome da coluna pk
        self.cursor.execute(f"SHOW KEYS FROM {table_to_update} WHERE Key_name = 'PRIMARY'")
        pk_name = db.cursor.fetchall()
        pk_name = pk_name[0][4]
        if self.cursor.execute(
                f"SELECT {pk_name} FROM {table_to_update} where {pk_name} = {id}") == 0:
            raise Exception(f"Id: {id} not found in table: {table_to_update}!")

        frase_set = self.convert_dict_to_sql_string(dict_values)
        comando = f"UPDATE {table_to_update} SET " + frase_set + f" WHERE {pk_name} = {id}"
        self.cursor.execute(comando)
        return True


    def crud_delete(self):
        pass


db = DataBase()
db.create_connection_and_cursor("hoteis_regioes")
db.crud_update("cidades", 12, dict(cidade="Santo Antonio da Platina"))
# table_to_update = "cidades"

# db.crud_read("cidades")
# titulo_id = (db.cursor.fetchall()[0][4])

# comando = "SELECT count(*) AS NUMBEROFCOLUMNS FROM information_schema.columns " \
"WHERE table_name ='{table_to_update}'"

# comando = "SELECT COUNT(*) as NumberofColumns FROM INFORMATION_SCHEMA.COLUMNS " \
#           "WHERE table_schema = 'hoteis_regioes' and table_name = 'cidades'"
# (db.cursor.execute(comando))
# print(db.cursor.fetchall())

# comando = "SELECT COUNT(*) as NumberofColumns FROM INFORMATION_SCHEMA.COLUMNS " \
#           "WHERE table_schema = 'hoteis_regioes' and table_name = 'cidades'"
# (db.cursor.execute(comando))
# print(db.cursor.fetchall())

#
# comando = "SELECT DATABASE()"
# db.cursor.execute("SELECT DATABASE()")
# print(db.cursor.fetchall()[0][0])
