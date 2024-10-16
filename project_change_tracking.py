import json
import subprocess

import flask
import re
import time

from flask import Blueprint, request, jsonify, url_for, flash
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from wtforms import Form, SelectField, StringField, BooleanField, TimeField, DateField
from wtforms.validators import InputRequired

from croniter import croniter, CroniterBadCronError, CroniterBadDateError

from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from airflow import settings
from airflow.models import Connection

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as MSSH
from airflow.providers.postgres.hooks.postgres import PostgresHook as PH
from airflow.providers.exasol.hooks.exasol import ExasolHook as EH
from airflow.hooks.base_hook import BaseHook as BH

bp = Blueprint(
    "ct_projects_administration",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/ct_projects_administration"
)


def validate_cron(form, field) -> bool:
    """Кастомный валидатор Cron выражений"""
    cron = field.data
    try:
        croniter(cron)
        return True
    except (CroniterBadCronError, CroniterBadDateError):
        return False


def replace_response_data(raw_data_field: str) -> str:
    """Функция преобразования отображения данных для запроса к базе данных"""

    if raw_data_field is None or raw_data_field == " ":
        return 'NULL'
    else:
        return f"'{raw_data_field}'"


class GetConnection:
    """Класс для получения connections"""

    @staticmethod
    def get_all_connections() -> list:
        """Получаем все Connections из Apache Airflow"""
        session = settings.Session()
        connections = session.query(Connection).all()
        connections_list = [i.conn_id for i in connections]
        return connections_list

    @staticmethod
    def get_database_connection(name_database: str) -> list:
        """
        Получаем определенный Connection из Apache Airflow

        params:: ['exasol', 'postgres', 'mssql', 'mysql']
        """
        database_alias = ''
        if name_database == 'MSSQL':
            database_alias = 'mssql'
        elif name_database == 'PostgreSQL':
            database_alias = 'postgres'
        elif name_database == 'Exasol':
            database_alias = 'exasol'
        elif name_database == 'MYSQL':
            database_alias = 'mysql'
        session = settings.Session()
        connections = session.query(Connection).all()
        connections_list = [i.conn_id for i in connections if database_alias in i.conn_type]
        return connections_list

    @staticmethod
    def get_permissions_about_bd_user(database_name: str, database_type: str, connection_id: str,
                                      permissions: list | str) -> True | False:
        conn = BH.get_connection(connection_id)

        username = conn.login
        print(username)

        sql_query_permissions = ''

        if database_type == 'PostgreSQL':
            sql_query_permissions = f"""
                SELECT
                    privilege_type
                FROM information_schema.role_table_grants
                WHERE {database_name} AND grantee = {username}
            """

        elif database_type == "MSSQL":
            sql_query_permissions = f"""
                USE {database_name};
                SELECT permission_name FROM fn_my_permissions(NULL, 'DATABASE')
                WHERE permission_name in ('SELECT', 'INSERT', 'UPDATE', 'DELETE');
            """

        with GetDatabase.get_hook_for_database(database_type=database_type, conn_id=connection_id).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query_permissions)
                rows = cursor.fetchall()
                raw_permissions = [row[0] for row in rows]

        print("!" * 30)
        print(raw_permissions)
        print("!" * 30)

        if isinstance(permissions, list):
            if all(elem in raw_permissions for elem in permissions):
                return True
            else:
                return False

        if isinstance(permissions, str):
            if permissions in raw_permissions:
                return True
            else:
                return False


class GetDatabase:
    """Получение всех баз данных по connection"""

    @staticmethod
    def get_all_database_mssql(connection: str) -> list[str]:
        """Получение connections из базы данных mssql"""
        mssql_hook = MSSH(mssql_conn_id=connection)
        sql = "SELECT name, database_id FROM sys.databases;"
        databases = [i[0] for i in mssql_hook.get_records(sql)]
        return databases

    @staticmethod
    def get_all_database_postgres(connection: str) -> list[str]:
        """Получение connections из базы данных postgres"""
        pg_hook = PH.get_hook(connection)
        sql = "SELECT datname FROM pg_database;"
        databases = [i[0] for i in pg_hook.get_records(sql)]
        return databases

    @staticmethod
    def get_all_schemas_exasol(connection: str) -> list[str]:
        """Получение connections из базы данных exasol"""
        exasol_hook = EH(exasol_conn_id=connection)
        sql = "SELECT SCHEMA_NAME FROM EXA_ALL_SCHEMAS;"
        databases = [i[0] for i in exasol_hook.get_records(sql)]
        return databases

    @staticmethod
    def get_connection_postgres():
        """Получение хука Postgres"""
        pg_hook = PH.get_hook("airflow_postgres")
        return pg_hook

    @staticmethod
    def get_hook_for_database(database_type: str, conn_id: str) -> str:
        """Получение хуков по типу базы данных и коннекшену"""
        hook = 'WRONG!'
        if database_type == 'MSSQL':
            hook = MSSH(mssql_conn_id=conn_id)
        elif database_type == 'PostgreSQL':
            hook = PH.get_hook(conn_id)
        # elif database_type == 'Exasol':
        #     hook = EH(exasol_conn_id=conn_id)
        return hook


class ProjectForm(Form):
    """Форма администрирования CT Projects"""

    source_database_type = SelectField(
        "Source Database Type",
        choices=[" ", "MSSQL", "PostgreSQL"],
        id="conn_type source_database_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   }
    )

    source_connection_id = SelectField(
        'Source Connection ID',
        validators=[InputRequired()],
        choices=[],
        id="conn_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    project_database = SelectField(
        'Project Database',
        validators=[InputRequired()],
        choices=[],
        id="conn_type project_database",
        name="conn_type project_database",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    source_database = SelectField(
        'Source Database',
        validators=[InputRequired()],
        choices=[],
        id="conn_type source_database",
        name="conn_type source_database",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    is_source_1c = BooleanField(
        'Is Source 1C?',
        false_values=(False, 'NO', 'YES')
    )

    biview_database = SelectField(
        'BIView Database',
        validators=[InputRequired()],
        choices=[],
        id="conn_type biview_database",
        name="conn_type biview_database",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    # biview_project_type = RadioField(
    #     'BIView Project Type',
    #     validators=[InputRequired()],
    #     choices=[('1', 'Type 1'), ('2', 'Type 2')],
    #     default='1',
    #     name="project_type",
    #     id="project_type",
    #     render_kw={"class": "form-check-input",
    #                "type": "radio"
    #                }
    # )

    transfer_source_data = BooleanField(
        'Transfer Source Data',
        false_values=(False, 'NO', 'YES')
    )

    target_database_type = SelectField(
        "Target Database Type",
        choices=[" ", "Exasol", "MYSQL"],
        id="conn_type target_database_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   }
    )

    target_connection_id = SelectField(
        'Target Connection ID',
        choices=[],
        id="conn_type target_connection_id",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    target_schema = SelectField(
        'Target Schema',
        id="conn_type target_schema",
        name="conn_type target_schema",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    target_type = SelectField(
        'Target Type',
        default=' ',
        choices=['ODS', 'HODS'],
        id="conn_type target_type",
        name="conn_type target_type",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    update_dags_start_date = DateField('Start Date (UTC)',
                                       render_kw={"class": "form-control-short"}
                                       )
    update_dags_start_time = TimeField('Start time')

    update_dags_schedule = StringField('Schedule',
                                       validators=[validate_cron],
                                       id="schedule",
                                       render_kw={"class": "form-control-short",
                                                  "placeholder": "* * * * *"
                                                  }
                                       )

    transfer_dags_start_date = DateField('Start Date (UTC)',
                                         render_kw={"class": "form-control-short"}
                                         )

    transfer_dags_start_time = TimeField('Start time')

    transfer_dags_schedule = StringField('Schedule',
                                         validators=[validate_cron],
                                         id="schedule",
                                         render_kw={"class": "form-control-short",
                                                    "placeholder": "* * * * *"
                                                    }
                                         )


class ProjectsView(AppBuilderBaseView):
    """Представление CT Projects"""
    default_view = "project_list"

    @expose('/', methods=['GET'])
    def project_list(self):
        """Представление списка проектов"""

        sql_query = """
                        SELECT
                            source_database_type,
                            source_connection_id,
                            project_database,
                            source_database,
                            is_source_1c,
                            biview_database,
                            transfer_source_data,
                            target_database_type,
                            target_connection_id,
                            target_schema,
                            target_type 
                        FROM airflow.atk_ct.ct_projects
                    """

        columns = [field.label.text for field in ProjectForm()][:11]
        print(columns)
        with GetDatabase.get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)

                try:
                    rows = cursor.fetchall()
                    raw_projects = [dict(zip(columns, row)) for row in rows]

                    projects = []

                    keys_to_clean = [
                        "BIView Database",
                        "Target Database Type",
                        "Target Connection ID",
                        "Target Schema",
                        "Target Type"
                    ]

                    for dictionary in raw_projects:
                        for key in keys_to_clean:
                            if dictionary.get(key) in ['NULL', None]:
                                dictionary[key] = ''

                        dictionary['Transfer Source Data'] = 'Yes' if dictionary.get('Transfer Source Data') else 'No'
                        dictionary['Is Source 1C?'] = 'Yes' if dictionary.get('Is Source 1C?') else 'No'

                        projects.append(dictionary)

                except Exception as e:
                    flash(str(e), category="error")
        return self.render_template("project_change_tracking.html",
                                    projects=projects,
                                    count_projects=len(raw_projects))

    @expose("/add", methods=['GET', 'POST'])
    @csrf.exempt
    def project_add_data(self):
        """Добавление CT Project"""

        form = ProjectForm()

        if request.method == 'POST':

            form_add = ProjectForm(request.form)

            sql_insert_query = f"""
                                INSERT INTO airflow.atk_ct.ct_projects (
                                    source_database_type,
                                    source_connection_id,
                                    project_database,
                                    source_database,
                                    is_source_1c,
                                    biview_database,
                                    transfer_source_data,
                                    target_database_type,
                                    target_connection_id,
                                    target_schema,
                                    target_type,
                                    update_dags_start_date,
                                    update_dags_start_time,
                                    update_dags_schedule,
                                    transfer_dags_start_date,
                                    transfer_dags_start_time,
                                    transfer_dags_schedule
                                    )
                                VALUES (
                                    '{form_add.source_database_type.data}',
                                    '{form_add.source_connection_id.data}',
                                    '{form_add.project_database.data}',
                                    '{form_add.source_database.data}',
                                    '{form_add.is_source_1c.data}',
                                    {replace_response_data(form_add.biview_database.data)},
                                    {form_add.transfer_source_data.data},
                                    {replace_response_data(form_add.target_database_type.data)},
                                    {replace_response_data(form_add.target_connection_id.data)},
                                    {replace_response_data(form_add.target_schema.data)},
                                    {replace_response_data(form_add.target_type.data)},
                                    {replace_response_data(form_add.update_dags_start_date.data)},
                                    {replace_response_data(form_add.update_dags_start_time.data)},
                                    '{form_add.update_dags_schedule.data}',
                                    {replace_response_data(form_add.transfer_dags_start_date.data)},
                                    {replace_response_data(form_add.transfer_dags_start_time.data)},
                                    '{form_add.transfer_dags_schedule.data}'
                                    );
                                """

            try:
                if not GetConnection.get_permissions_about_bd_user(database_name=form_add.project_database.data,
                                                                   database_type=form_add.source_database_type.data,
                                                                   connection_id=form_add.source_connection_id.data,
                                                                   permissions='INSERT'):
                    raise ValueError("The user does not have permission to create tables!")

                if form_add.source_database_type == " " or form_add.target_database_type == " ":
                    raise ValueError("Invalid value for database type!")

                with GetDatabase.get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_insert_query)
                    conn.commit()

                flash("The project has been saved successfully.", category="info")

                return jsonify({
                    'success': True,
                    'message': 'The project has been saved successfully.',
                    'redirect': url_for('ProjectsView.edit_project_data',
                                        project_database=form_add.project_database.data)
                })

            except Exception as e:

                if 'duplicate key' in str(e):
                    flash("This project database already exists! Choose another one.", category='warning')
                    message = "This project database already exists! Choose another one."
                elif 'not able to access' in str(e):
                    flash('The user does not have permission to create tables!', category='warning')
                    message = "The user does not have permission to create tables!"
                else:
                    flash(str(e), category='warning')
                    message = str(e)

                return jsonify({
                    'success': False,
                    'message': message,
                    'redirect': url_for('ProjectsView.project_add_data')
                })
        return self.render_template("add_projects.html", form=form)

    @expose("/edit/<string:project_database>", methods=['GET', 'POST'])
    @csrf.exempt
    def edit_project_data(self, project_database):
        """Редактирование CT Project"""

        sql_select_query = """SELECT * FROM airflow.atk_ct.ct_projects WHERE project_database = %s;"""

        with GetDatabase.get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select_query, (project_database,))
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                projects_data = [dict(zip(columns, row)) for row in rows]

        form_exist = ProjectForm(data=projects_data[0])

        form_update = ProjectForm(request.form)

        if request.method == 'POST':

            sql_update_query = f"""
                                UPDATE airflow.atk_ct.ct_projects
                                SET source_database = '{form_update.source_database.data}',
                                    is_source_1c = {form_update.is_source_1c.data},
                                    biview_database = {replace_response_data(form_update.biview_database.data)},
                                    transfer_source_data = {form_update.transfer_source_data.data},
                                    target_database_type = {replace_response_data(
                form_update.target_database_type.data)},
                                    target_connection_id = {replace_response_data(
                form_update.target_connection_id.data)},
                                    target_schema = {replace_response_data(form_update.target_schema.data)},
                                    target_type = {replace_response_data(form_update.target_type.data)},
                                    update_dags_start_date = {replace_response_data(
                form_update.update_dags_start_date.data)},
                                    update_dags_start_time = {replace_response_data(
                form_update.update_dags_start_time.data)},
                                    update_dags_schedule = '{form_update.update_dags_schedule.data}',
                                    transfer_dags_start_date = {replace_response_data(
                form_update.transfer_dags_start_date.data)},
                                    transfer_dags_start_time = {replace_response_data(
                form_update.transfer_dags_start_time.data)},
                                    transfer_dags_schedule = '{form_update.transfer_dags_schedule.data}'
                                WHERE project_database = '{project_database}'
                                ;"""
            print(sql_update_query)
            print(form_update.project_database.data, form_update.source_database_type.data,
                  form_update.source_connection_id.data)
            try:
                if not GetConnection.get_permissions_about_bd_user(database_name=form_exist.project_database.data,
                                                                   database_type=form_exist.source_database_type.data,
                                                                   connection_id=form_exist.source_connection_id.data,
                                                                   permissions=['INSERT', 'UPDATE']):
                    raise ValueError("The user does not have permission to change tables!")

                if form_update.source_database_type == " " or form_update.target_database_type == " ":
                    raise ValueError("Некорректное значение для типа базы данных!")

                with GetDatabase.get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_update_query)
                    conn.commit()

                flash("The project has been successfully modified!", category="info")

                return jsonify({'success': True, 'message': 'The project has been saved successfully.',
                                'redirect': url_for('ProjectsView.edit_project_data',
                                                    project_database=project_database)})
                # return flask.redirect(url_for('ProjectsView.edit_project_data', project_database=project_database))

            except Exception as e:

                if 'duplicate key' in str(e):
                    flash("This project database already exists! Choose another one.", category='warning')
                    message = "This project database already exists! Choose another one."
                elif 'not able to access' in str(e):
                    flash('The user does not have permission to create tables!', category='warning')
                    message = "The user does not have permission to create tables!"
                else:
                    flash(str(e), category='warning')
                    message = str(e)

                return jsonify({
                    'success': False,
                    'message': message,
                    'redirect': url_for('ProjectsView.edit_project_data',
                                        project_database=project_database)
                })

        return self.render_template("edit_project.html", form=form_exist)

    @expose('/delete/<string:project_database>', methods=['GET'])
    @csrf.exempt
    def delete_ct_project(self, project_database):
        """Удаление CT Project"""
        source_database_type = request.args.get("database_type")
        source_connection_id = request.args.get("connection_id")
        print(source_database_type)
        print(source_connection_id)
        sql_delete_query = """DELETE FROM airflow.atk_ct.ct_projects WHERE project_database = %s"""

        try:
            # if not GetConnection.get_permissions_about_bd_user(database_name=project_database,
            #                                                    database_type=source_database_type,
            #                                                    connection_id=source_connection_id,
            #                                                    permissions="DELETE"):
            #     raise ValueError("The user does not have permission to delete tables!")

            with GetDatabase.get_connection_postgres().get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_delete_query, (project_database,))
                conn.commit()

            flash("Project successfully deleted!", category="info")

            try:
                sql_execute_query = f"""
                EXECUTE [ct__config].[dbo].[ct__delete_project_from_metadata]
                    @ct_db_name = {project_database},
                    @ct_schema_name = 'dbo';
                                    """

                with GetDatabase.get_hook_for_database(source_database_type, source_connection_id).get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_execute_query)
                    conn.commit()

                # time.sleep(2)

            except Exception as e:

                pattern = r'"([^"]*?@[\w_]+)"'
                wrong_field = re.findall(pattern, str(e))

                if not wrong_field:
                    if 'error message 20018' in str(e):
                        message = "Invalid database selected for table generation!"
                        flash(message, category="warning")
                    else:
                        message = str(e)
                else:
                    message = f'Wrong field {wrong_field[0][1:]}!'

                flash(message, category="warning")

        except Exception as e:
            flash(str(e))

        return flask.redirect(url_for('ProjectsView.project_list'))

    @expose('/projects_to_load', methods=['GET'])
    def projects_to_load(self):
        """Отображение списка таблиц"""
        project_database = request.args.get('project_database')
        biview_database = request.args.get('biview_database')
        source_database = request.args.get('source_database')
        source_database_type = request.args.get('source_database_type')
        connection = request.args.get('connection')

        project_data = {
            'connection': connection,
            'project_database': project_database,
            'biview_database': biview_database,
            'source_database': source_database,
            'source_database_type': source_database_type
        }

        return self.render_template("projects_to_load.html", project_data=project_data)

    # <-------------- API Endpoints -------------->

    @expose('/api/get_all_projects/', methods=['GET'])
    def get_all_projects(self) -> json:
        """Эндпоинт возвращает json с данными о всех проектах"""

        sql_query = """
                        SELECT
                            project_database
                        FROM airflow.atk_ct.ct_projects
                    """

        with GetDatabase.get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)

                try:
                    rows = cursor.fetchall()
                    list_of_row = [row[0] for row in rows]
                    raw_projects = {"Projects databases": list_of_row}

                except Exception as e:
                    flash(str(e), category="error")

        return jsonify(raw_projects)

    @expose('/api/get_connections/', methods=['GET'])
    def get_filtered_connections(self):
        """Эндпоинт возвращает json с connections соответствующих принимаемому типу базы данных"""

        database_type = request.args.get('database_type')
        if not database_type:
            return jsonify({'status': 'error', 'message': 'No data provided'}), 400
        connections = GetConnection.get_database_connection(database_type)
        return jsonify(connections)

    @expose("/api/get_source_database/", methods=['GET'])
    def get_source_database(self):
        """Эндпоинт возвращает json баз данных-источников соответствующих принимаемым connections"""

        databases = []
        get_connection = request.args.get('connection')
        database_is_biview = request.args.get('database_is_biview')

        session = settings.Session()
        connections = session.query(Connection).all()
        connection = [conn for conn in connections if conn.conn_id == get_connection][0]

        if connection.conn_type == 'mssql':

            if database_is_biview == "True":
                raw_databases = GetDatabase.get_all_database_mssql(connection.conn_id)

                databases = []

                for database in raw_databases:
                    try:
                        sql_query = f"""
                            USE {database};
                            SELECT name FROM SYS.TABLES
                            WHERE name in ('ATK_TRef', 'ATK_TField', 'ATK_TEnum')
                                AND SCHEMA_NAME(schema_id) = 'dbo'
                                AND type = 'U';
                        """

                        with GetDatabase.get_hook_for_database('MSSQL', get_connection).get_conn() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute(sql_query)
                                rows = cursor.fetchall()
                                all_tables_in_database = [row[0] for row in rows]

                        if all(elem in all_tables_in_database for elem in ('ATK_TRef', 'ATK_TField', 'ATK_TEnum')):
                            databases.append(database)

                    except Exception as e:
                        print(e)
                        continue

                return jsonify(databases)

            else:
                databases = GetDatabase.get_all_database_mssql(connection.conn_id)

        elif connection.conn_type == 'postgres':
            databases = GetDatabase.get_all_database_postgres(connection.conn_id)

        return jsonify(databases)

    @expose("/api/get_target_database/", methods=['GET'])
    def get_target_database(self):
        """Эндпоинт возвращает json целевых баз данных соответствующих принимаемым connections"""

        databases = []
        get_connection = request.args.get('connection')

        session = settings.Session()
        connections = session.query(Connection).all()
        connection = [conn for conn in connections if conn.conn_id == get_connection][0]

        if connection.conn_type == 'exasol':
            databases = GetDatabase.get_all_schemas_exasol(connection.conn_id)

        elif connection.conn_type == 'mysql':
            databases = GetDatabase.get_all_database_mssql(connection.conn_id)

        return jsonify(databases)

    @expose("/api/get_project_data/", methods=['GET'])
    def get_project_data(self):
        """Эндпоинт возвращает json данные о конкретном CT Project"""

        project_database = request.args.get('project_database')

        sql_select_query = """SELECT * FROM airflow.atk_ct.ct_projects WHERE project_database = %s;"""

        with GetDatabase.get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select_query, (project_database,))
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

                projects_data = [dict(zip(columns, row)) for row in rows][0]

        return jsonify(projects_data)

    @expose("/api/fetch_data", methods=['GET'])
    def fetch_data(self):
        """Эндпоинт возвращает json с данными о таблицах ct__tables из базы данных project_database"""

        project_database = request.args.get('project_database')
        connection_id = request.args.get('connection')
        source_database_type = request.args.get('source_database_type')

        try:
            sql_query = f"""
                           SELECT
                               table_alias,
                               load
                           FROM ct__config.dbo.ct__tables
                           WHERE exists_in_source = 1 AND ct_db = {project_database};
                        """

            with GetDatabase.get_hook_for_database(source_database_type, connection_id).get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_query)
                    rows = cursor.fetchall()

                    columns = [desc[0] for desc in cursor.description]
                    raw_projects = [dict(zip(columns, row)) for row in rows]

            response_data = {
                "status": "success",
                "columns": columns,
                "results": raw_projects
            }

            return jsonify(response_data)

        except Exception as e:
            if 'Invalid object name' in str(e):
                return jsonify({'status': 'error', 'message': 'Tables is not defined'})
            else:
                return jsonify({'status': 'error', 'message': str(e)})

    @expose("/api/update_data_is_load", methods=['POST'])
    @csrf.exempt
    def update_data_is_load(self):
        """Эндпоинт выполняет изменения в таблицах ct__tables"""

        try:
            data = request.get_json()
            changes = data.get('data')

            connection_id = request.args.get('connection')
            source_database_type = request.args.get('source_database_type')
            project_database = request.args.get('project_database')

            if not data:
                return jsonify({'status': 'error', 'message': 'No data provided'}), 400

            with GetDatabase.get_hook_for_database(source_database_type, connection_id).get_conn() as conn:
                with conn.cursor() as cursor:

                    try:
                        update_queries = []
                        for entry in changes:
                            print("entry")
                            print(entry)
                            table_alias = entry['table_alias']
                            for change in entry['changes']:
                                print("change")
                                print(change)
                                field = change['field']
                                new_value = int(change['newValue'])
                                print(new_value)

                                query = f"""
                                    UPDATE {project_database}.dbo.ct__tables
                                    SET {field} = %s
                                    WHERE table_alias = %s;
                                """

                                update_queries.append((query, (new_value, table_alias)))

                        for query, params in update_queries:
                            cursor.execute(query, params)
                        conn.commit()
                        return jsonify({'status': 'success'}), 200

                    except Exception as e:
                        conn.rollback()
                        print(f"Error occurred while updating data: {e}")
                        return jsonify({'status': 'error', 'message': str(e)}), 500

        except Exception as e:
            print(f"Error processing request: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @expose('/api/validate_db', methods=["GET"])
    def ct_validate_db(self):

        project_database = request.args.get('project_database')

        sql_query_project_data = """ SELECT
                                        source_database_type,
                                        source_connection_id,
                                        project_database,
                                        source_database,
                                        is_source_1c,
                                        biview_database,
                                        transfer_source_data,
                                        target_database_type,
                                        target_connection_id,
                                        target_schema,
                                        target_type 
                                    FROM airflow.atk_ct.ct_projects
                                    WHERE project_database = %s;
                                    """

        columns = [field.label.text for field in ProjectForm()][:11]
        with GetDatabase.get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query_project_data, (project_database,))
                rows = cursor.fetchall()
                project_data = [dict(zip(columns, row)) for row in rows][0]

        manage_tables = 0
        force_drop_tables = 0
        fld_1c_name_kind = 1

        source_db_type = project_data["Source Database Type"]
        source_db_name = project_data["Source Database"]
        ct_db_name = project_data["Project Database"]  # will change!

        if project_data['Is Source 1C?']:
            source_is_1c = 1
            biview_db_name = project_data["BIView Database"]
            biview_schema_name = 'dbo'
        elif not project_data['Is Source 1C?']:
            source_is_1c = 0
            biview_db_name = ""
            biview_schema_name = ""
        else:
            message = 'No one matches!'
            return jsonify({'status': 'error', 'message': message}), 500
        try:

            sql_execute_query = f""" EXECUTE [ct__config].[dbo].[ct__validatedb]
                                @source_db_name = {replace_response_data(source_db_name)},
                                @source_schema_name = 'dbo',
                                @source_db_type = {replace_response_data(source_db_type)},
                                @source_is_1c = {source_is_1c},
                                @biview_db_name = {replace_response_data(biview_db_name)},
                                @biview_schema_name = {replace_response_data(biview_schema_name)},
                                @ct_db_name = {replace_response_data(ct_db_name)},
                                @ct_schema_name = 'dbo',
                                @target_db_type = 'EXASOL',
                                @target_db_name = 'DB1',
                                @target_schema_name = 'STG_CDC',
                                @manage_tables = {manage_tables},
                                @force_drop_tables = {force_drop_tables},
                                @fld_1c_name_kind = {fld_1c_name_kind};
                                """

            with GetDatabase.get_hook_for_database(project_data['Source Database Type'],
                                                   project_data['Source Connection ID']).get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_execute_query)
                conn.commit()

            # time.sleep(2)

            return jsonify({'status': 'success', 'script': sql_execute_query}), 200

        except Exception as e:
            if 'error message 20018' in str(e):
                message = "Invalid database selected for table generation!"
                return jsonify({'status': 'error', 'message': message}), 500
            pattern = r'"([^"]*?@[\w_]+)"'
            wrong_field = re.findall(pattern, str(e))
            if not wrong_field:
                message = str(e)
            else:
                message = f'Wrong field {wrong_field[0][1:]}!'
            return jsonify({'status': 'error', 'message': message}), 500

    @expose('/api/dag_validate_db', methods=["GET"])
    def use_ct_validate_dag(self):

        project_database = request.args.get('project_database')

        command = [
            'airflow', 'dags', 'trigger', 'validate_ct_tables',
            '--conf',
            f'{{"project_database": "{project_database}"}}'
        ]

        try:
            subprocess.run(command, check=True)
            return jsonify({"status": "DAG triggered successfully"}), 200

        except subprocess.CalledProcessError as e:
            return jsonify({"status": "Error", "error": str(e)}), 500


v_appbuilder_view = ProjectsView()
v_appbuilder_package = {
    "name": "CT Projects",
    "category": "ATK Change Tracking",
    "view": v_appbuilder_view
}


class AirflowConnectionPlugin(AirflowPlugin):
    name = "project_list"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
