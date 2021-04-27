from app.database import *
from unittest import mock, TestCase
import MySQLdb

DB = ""
HOST = "localhost"
USER = "root"
PASSWORD = ""
PORT = 3306

class TestDataBase(TestCase):

    @mock.patch("app.database.MySQLdb")
    def test_create_connection_and_cursor_works(self, mock_mysql):
        mock_mysql.connect.return_value = MySQLdb.connect(host=HOST, user=USER, port=PORT)
        DataBase().create_connection_and_cursor()
        mock_mysql.connect.assert_called_once()

    def test_conn_and_cursor_exist(self):
        # Como não passo nada, nao vai fazer a conexão e vai voltar um False
        self.assertFalse(DataBase().conn_and_cursor_exist())

        #moco conn e cursor forçadamente (create = True) e, assim vai retornar True
        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            with mock.patch("app.database.DataBase.conn", create=True) as mock_conn:
                self.assertTrue(DataBase().conn_and_cursor_exist())

    def test_is_database_selected(self):
        #with o cursor mocado, vai retornar True
        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            mock_cursor.execute.return_value = None #poderia ser qualquer coisa (None, 1, 0... pq tá mocado)
            self.assertTrue(DataBase().is_database_selected())

        # sem passar nada. map vai conseguir conectar com o db (e dar False)
        self.assertFalse(DataBase().is_database_selected())

    # VEJA QUE ESSA FUNÇÃO NÃO TEM RETORNO...
    # ENTÃO EU SÓ PASSEI POR ELA (GARANTI QUE O TESTE PASSE POR ELA)
    @mock.patch("app.database.DataBase.conn", create=True)
    def test_change_current_database(self, mock_conn):
        mock_conn.select_db.return_value = None
        DataBase().change_current_database("")

    #VEJA QUE A ORDEM QUE EU CHAMO OS MOCK AQUI É O INVERSO
    #ao invés de conn_and_cursor_exist e depois is_database_selected... eu inverto)
    @mock.patch("app.database.DataBase.is_database_selected")
    @mock.patch("app.database.DataBase.conn_and_cursor_exist")
    def test_up_to_table_is_ok(self, mock_conn_and_cursor_exist, mock_is_database_selected):
        # Mock conn and cursor não existem e vai levantar o primeiro Exception
        # que é ("Connection or cursor is not defined!")
        mock_conn_and_cursor_exist.return_value = False

        # with assertRaises que vai puxar uma exception, eu tento fazer
        # o procedimento de insert_data e vai dar erro pq não tem conn e cursor
        with self.assertRaises(Exception) as error:
            DataBase().insert_data("", [])
            # esse insert_data vai precisar mudar de lista para dict quando eu
            # adaptar a função do Gustavo.
        #Aqui eu checo se o error que foi carregado bate com a frase que eu escrevi (e espero)
        self.assertEqual("Connection or cursor is not defined!", error.exception.args[0])

        # Aqui eu garanto que conn_and_cursor_exist exista e que
        # is_data_base_selected seja False
        mock_conn_and_cursor_exist.return_value = True
        mock_is_database_selected.return_value = False
        # com o with, eu checo se qualquer função chamada (insert_data) vai logar um erro
        # e vejo se a mensagem de erro é a mesma
        with self.assertRaises(Exception) as error:
            DataBase().insert_data("", []) #precisava passar nome de tabela e lista
        self.assertEqual("Database is not selected!", error.exception.args[0])

        # agora eu transformo o is_database_selected em True para as próximas etapas
        mock_is_database_selected.return_value = True

        # com conn_and_cursor_exists e is_database_selected funcionando,
        # eu forço o cursor.execute a retornar None (e não funcionar) para cair no Exception
        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            mock_cursor.execute.return_value = None
            with self.assertRaises(Exception) as error:
                DataBase().up_to_table_is_ok("") #precisava passar nome de tabela
                self.assertEqual("Table:  not found!", error.exception.args[0])

            # Com tudo certinho, eu garanto que retonre True
            mock_cursor.execute.return_value = 1
            self.assertTrue(DataBase().up_to_table_is_ok(""))

    def test_convert_list_to_sql_string_works(self):
        list_to_test = ["default", 0, 0.5, "Gustavo", "2021-05-02"]
        result = DataBase().convert_list_to_sql_string(list_to_test)
        self.assertEqual(result, "default,0,0.5,'Gustavo','2021-05-02'")

    def test_convert_dict_to_sql_string(self):
        #checo se os dados passados por um dicionário com separador "and" viram a frase correta
        dict_to_test = dict(nome="Ana Maria", idade= 25, nasc="2021-05-02", casado=0, separator= " and ")
        result = DataBase().convert_dict_to_sql_string(dict_to_test)
        self.assertEqual(result, "nome = 'Ana Maria' and idade = 25 and nasc = '2021-05-02' and casado = 0")

        # # checo se os dados passados por um dicionário com separador "," viram a frase correta
        # dict_to_test = dict(nome="Ana Maria", idade= 25, nasc="2021-05-02", casado=0, separator= " , ")
        # result = DataBase().convert_dict_to_sql_string(dict_to_test)
        # self.assertEqual(result, "nome = 'Ana Maria' , idade = 25 , nasc = '2021-05-02' , casado = 0")


    def test_finds_pk_table_name(self):
        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            mock_cursor.execute.return_value = 1
            mock_cursor.fetchall.return_value = ()
            pk_name = ""
            self.assertEqual(pk_name, DataBase().finds_pk_table_name(""))

    @mock.patch("app.database.Database.up_to_table_is_ok")
    def test_insert_data_works(self):

        with self.assertRaises(TypeError) as error:
            DataBase().insert_data("", "")
        self.assertEqual("Data is not a list!", error.exception.args[0])

        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            mock_cursor.execute.side_effect = [0, 1]
            self.assertEqual(DataBase().insert_data("", []), False)
            self.assertEqual(DataBase().insert_data("", []), True)

        self.assertFalse(DataBase().insert_data("", []))

    @mock.patch("app.database.DataBase.up_to_table_is_ok")
    def test_crud_read(self, mock_table_is_ok):
        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            #forço que ele entenda que conseguiu selecionar toda a tabela
            mock_cursor.execute.return_value= 1

            # aqui, com esse () ele entende que o df veio vazio
            mock_cursor.fetchall.return_value = ()
            DataBase().crud_read("")

    @mock.patch("app.database.DataBase.up_to_table_is_ok")
    @mock.patch("app.database.DataBase.finds_pk_table_name")
    @mock.patch("app.database.DataBase.convert_dict_to_sql_string")
    def test_crud_update(self, mock_convert_to_sql, mock_finds_pk_table_name, mock_table_is_ok):
        #mocando a frase pk_name
        mock_finds_pk_table_name.return_value = ""
        #mocando frase_set
        mock_convert_to_sql.return_value = ""

        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            # forço o cursor a retornar zero para garantir que
            # eu caia na exceção e checo se a mensagem de erro é igual
            # a mensagem que eu criei
            mock_cursor.execute.return_value = 0
            with self.assertRaises(Exception) as error:
                DataBase().crud_update("", "", {})
            self.assertEqual("Id:  not found in table: !", error.exception.args[0])

            # mando 1 para o primeiro e segundo cursor
            # 1ª cursor = validar que id existe
            # 2ª cursor diz que affected rows é maior que 0.
            mock_cursor.execute.side_effect = [1, 1]
            self.assertTrue(DataBase().crud_update("","", {}))

            # mando [1,0]: para o primeiro cursor aceitar que
            # id existe e 0 para dizer para o segundo cursor que
            # affected_rows é igual a 0
            mock_cursor.execute.side_effect = [1, 0]
            self.assertFalse(DataBase().crud_update("","", {}))

            # mando [1, ""]: 1 para dizer que o id existe e uma
            # string para forçar o segundo cursor cair no except
            # e devolver False
            mock_cursor.execute.side_effect = [1, ""]
            self.assertFalse(DataBase().crud_update("","", {}))

    @mock.patch("app.database.DataBase.up_to_table_is_ok")
    @mock.patch("app.database.DataBase.convert_dict_to_sql_string")
    def test_crud_delete(self, mock_convert_to_sql, mock_table_is_ok):
        #mocando a frase string_values
        mock_convert_to_sql.return_value = ""

        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            #forço a criação do cursor para 0 e 1.
            mock_cursor.execute.side_effect = [0, 1]
            self.assertFalse(DataBase().crud_delete("", {}))
            self.assertTrue(DataBase().crud_delete("", {}))
        #quando nao crio o cursor.
        self.assertFalse(DataBase().crud_delete("", {}))

    # @classmethod
    # def setUpClass(cls) -> None:
    #     print("Inicio")
    #
    # @classmethod
    # def tearDownClass(cls) -> None:
    #     print("final")
    #
    # def setUp(self) -> None:
    #     print("A")
    #
    # def tearDown(self) -> None:
    #     print("B")
