import sqlite3


class EditUconf:
    """Класс для работы с двумя базами данных SQLite."""

    def __init__(self, filename1=None, filename2=None):
        """Инициализация подключения к базам данных.

        Args:
            filename1 (str, optional): Путь к первой базе данных. Defaults to
            None.
            filename2 (str, optional): Путь ко второй базе данных. Defaults to
            None.
        """
        self.filename1 = filename1
        self.filename2 = filename2
        self.connection1 = None
        self.cursor1 = None
        self.connection2 = None
        self.cursor2 = None

    def connect(self, filepath1=None, filepath2=None):
        """Устанавливает соединение с базами данных.

        Args:
            filepath1 (str, optional): Путь к первой базе, если не указан при
            инициализации.
            filepath2 (str, optional): Путь ко второй базе, если не указан при
            инициализации.
        """
        try:
            if self.filename1 is None and self.filename2 is None:
                if filepath1 is None or filepath2 is None:
                    filepath1, filepath2 = input(), input()
                self.filename1 = rf'{filepath1}'
                self.filename2 = rf'{filepath2}'

            self.connection1 = sqlite3.connect(self.filename1)
            self.cursor1 = self.connection1.cursor()
            self.connection2 = sqlite3.connect(self.filename2)
            self.cursor2 = self.connection2.cursor()
            print('Соединение с базами данных установлено')
        except sqlite3.Error as e:
            print(f'Ошибка при подключении к базам: {e}')

    @staticmethod
    def _close_connection(conn, cursor, db_name):
        """Закрывает соединение с базой данных.

        Args:
            conn: Объект соединения с базой данных
            cursor: Объект курсора базы данных
            db_name (str): Имя базы данных для сообщений

        Returns:
            bool: True если соединение успешно закрыто, иначе False
        """
        try:
            if conn:
                if cursor:
                    cursor.close()
                conn.close()
                print(f'Соединение с базой данных {db_name} разорвано')
                return True
        except sqlite3.Error as e:
            print(f'Ошибка при закрытии соединения с {db_name}: {e}')
        return False

    def disconnect(self):
        """Закрывает все соединения с базами данных."""
        closed1 = EditUconf._close_connection(
            self.connection1, self.cursor1,
            f"'{self.filename1}' (первая БД)"
        )
        closed2 = EditUconf._close_connection(
            self.connection2, self.cursor2,
            f"'{self.filename2}' (вторая БД)"
        )

        if closed1:
            self.connection1 = None
            self.cursor1 = None
        if closed2:
            self.connection2 = None
            self.cursor2 = None

    def rw_uconf_tables(self, table_names):
        """Копирует данные из таблиц первой базы во вторую.

        Args:
            table_names (str or list): Имя таблицы или список таблиц для
            копирования

        Raises:
            sqlite3.Error: Если произошла ошибка при работе с базой данных
        """
        # Приводим к списку, если передана строка
        tables = [table_names] if isinstance(table_names, str) else table_names

        for table_name in tables:
            try:
                # Получаем данные из исходной таблицы
                self.cursor1.execute(f"SELECT * FROM {table_name}")
                rows = self.cursor1.fetchall()

                if not rows:
                    print(f"Таблица {table_name} пуста, пропускаем")
                    continue

                # Очищаем целевую таблицу
                self.cursor2.execute(f"DELETE FROM {table_name}")

                # Формируем плейсхолдеры для параметризованного запроса
                placeholders = ', '.join(['?'] * len(rows[0]))

                # Выполняем пакетную вставку
                self.cursor2.executemany(
                    f"INSERT INTO {table_name} VALUES ({placeholders})",
                    rows
                )

                # Фиксируем изменения
                self.connection2.commit()
                print(
                    f"Данные таблицы {table_name} успешно обновлены "
                    f"({len(rows)} записей)"
                )

            except sqlite3.Error as e:
                print(f"Ошибка при обновлении таблицы {table_name}: {e}")
                if self.connection2:
                    self.connection2.rollback()
                raise


# Создаем экземпляр класса для работы с базами данных
db_worker = EditUconf()

# Устанавливаем соединение с базами данных
db_worker.connect(
    filepath1='C:/Users/dfa.DEP/Downloads/uconf1.db',
    filepath2='C:/Users/dfa.DEP/Downloads/uconf2.db'
)

# Список таблиц для обработки (оставляем пустым)
tables_to_process = ['alg_cfc_io', 'alg_io', 'algs', 'algs_cfc', 'matrix_alg',
                     'matrix_alg_cfc', 'matrix_signals']

# Выполняем перенос данных между таблицами
db_worker.rw_uconf_tables(tables_to_process)

# Закрываем соединения
db_worker.disconnect()
