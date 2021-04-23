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
        self.assertFalse(DataBase().conn_and_cursor_exist())

        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            with mock.patch("app.database.DataBase.conn", create=True) as mock_conn:
                self.assertTrue(DataBase().conn_and_cursor_exist())

    def test_is_database_selected(self):
        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            mock_cursor.execute.return_value = None
            self.assertTrue(DataBase().is_database_selected())

        self.assertFalse(DataBase().is_database_selected())

    @mock.patch("app.database.DataBase.conn", create=True)
    def test_change_current_database(self, mock_conn):
        mock_conn.select_db.return_value = None
        DataBase().change_current_database("")

    def test_convert_list_to_sql_string_works(self):
        list_to_test = ["default", 0, 0.5, "Gustavo", "2021-05-02"]

        result = DataBase().convert_list_to_sql_string(list_to_test)
        self.assertEqual(result, "default,0,0.5,'Gustavo','2021-05-02'")

    @mock.patch("app.database.DataBase.is_database_selected")
    @mock.patch("app.database.DataBase.conn_and_cursor_exist")
    def test_insert_data_works(self, mock_conn_and_cursor_exist, mock_is_database_selected):

        mock_conn_and_cursor_exist.return_value = False
        with self.assertRaises(Exception) as error:
            DataBase().insert_data("", [])

        self.assertEqual("Connetion or cursor is not defined!", error.exception.args[0])

        mock_conn_and_cursor_exist.return_value = True

        # ///////////////////////////////////////////////////////////////////////////

        mock_is_database_selected.return_value = False
        with self.assertRaises(Exception) as error:
            DataBase().insert_data("", [])

        self.assertEqual("Database is not selected!", error.exception.args[0])

        mock_is_database_selected.return_value = True

        # ///////////////////////////////////////////////////////////////////////////

        with self.assertRaises(TypeError) as error:
            DataBase().insert_data("", "")

        self.assertEqual("Data is not a list!", error.exception.args[0])

        # ///////////////////////////////////////////////////////////////////////////

        with mock.patch("app.database.DataBase.cursor", create=True) as mock_cursor:
            mock_cursor.execute.side_effect = [0, 1]
            self.assertEqual(DataBase().insert_data("", []), False)
            self.assertEqual(DataBase().insert_data("", []), True)

        self.assertFalse(DataBase().insert_data("", []))



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
