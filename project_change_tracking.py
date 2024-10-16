from typing import Dict, Any, List

import flask
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, jsonify, url_for, flash
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow.utils.session import provide_session
from wtforms import Form, SelectField, RadioField, StringField, BooleanField, DateTimeLocalField, TimeField, DateField
from airflow.www.app import csrf
from wtforms.validators import InputRequired
from croniter import croniter, CroniterBadCronError, CroniterBadDateError

from airflow import settings
from airflow.models import Connection
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook as MSSH
from airflow.providers.postgres.hooks.postgres import PostgresHook as PH
from airflow.providers.exasol.hooks.exasol import ExasolHook as EH


#  Инициализация фронт-части плагина
bp = Blueprint(
    "project_data_saving",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/project_data_saving"
)


class GetConnection:
    """Класс для получения наборов connections"""

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


def get_hook_for_database(database_type: str, conn_id: str) -> str:
    hook = ''
    if database_type == 'MSSQL':
        hook = MSSH(mssql_conn_id=conn_id)
    elif database_type == 'PostgreSQL':
        hook = PH.get_hook(conn_id)
    # elif database_type == 'Exasol':
    #     hook = EH(exasol_conn_id=conn_id)
    return hook


def get_connection_postgres():
    """Получение хука Postgres"""
    pg_hook = PH.get_hook("airflow_postgres")
    return pg_hook


def validate_cron(form, field) -> bool:
    """Кастомный валидатор Cron выражений"""
    cron = field.data
    try:
        croniter(cron)
        return True
    except (CroniterBadCronError, CroniterBadDateError):
        return False


def replace_response_datetime(raw_datetime: str) -> str:
    if raw_datetime is None:
        return 'NULL'
    else:
        return f"'{raw_datetime}'"


def replace_response_data(raw_data_field: str) -> str:
    if raw_data_field is None or raw_data_field == " ":
        return 'NULL'
    else:
        return f"'{raw_data_field}'"


class ProjectForm(Form):
    """Form administration of ct project"""

    source_database_type = SelectField(
        "Source Database Type",
        choices=[" ", "MSSQL", "PostgreSQL"],
        id="conn_type6",
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
        id="conn_type3",
        name="conn_type3",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    source_database = SelectField(
        'Source Database',
        validators=[InputRequired()],
        choices=[],
        id="conn_type1",
        name="conn_type1",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    biview_database = SelectField(
        'BIView Database',
        validators=[InputRequired()],
        choices=[],
        id="conn_type2",
        name="conn_type2",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    biview_project_type = RadioField(
        'BIView Project Type',
        validators=[InputRequired()],
        choices=[('1', 'Type 1'), ('2', 'Type 2')],
        default='1',
        name="project_type",
        id="project_type",
        render_kw={"class": "form-check-input",
                   "type": "radio"
                   }
    )

    transfer_source_data = BooleanField(
        'Transfer Source Data',
        false_values=(False, 'NO', 'YES')
    )

    target_database_type = SelectField(
        "Target Database Type",
        choices=[" ", "Exasol", "MYSQL"],
        id="conn_type7",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   }
    )

    target_connection_id = SelectField(
        'Target Connection ID',
        choices=[],
        id="conn_type8",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value",
                   },
    )

    target_schema = SelectField(
        'Target Schema',
        id="conn_type4",
        name="conn_type4",
        render_kw={"class": "form-control",
                   "data-placeholder": "Select Value"
                   }
    )

    target_type = SelectField(
        'Target Type',
        default=' ',
        choices=['ODS', 'HODS'],
        id="conn_type5",
        name="conn_type5",
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
    """View of projects"""
    default_view = "project_list"

    @expose('/', methods=['GET'])
    def project_list(self):
        """View list of projects"""

        sql_query = """
                        SELECT
                            source_database_type,
                            source_connection_id,
                            project_database,
                            source_database,
                            biview_database,
                            biview_project_type,
                            transfer_source_data,
                            target_database_type,
                            target_connection_id,
                            target_schema,
                            target_type 
                        FROM airflow.atk_ct.ct_projects
                    """

        columns = [field.label.text for field in ProjectForm()][:11]
        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)

                try:
                    rows = cursor.fetchall()
                    raw_projects = [dict(zip(columns, row)) for row in rows]

                    projects = []
                    print(raw_projects)
                    for dictionary in raw_projects:

                        if dictionary["Target Database Type"] == 'NULL' or dictionary["Target Database Type"] is None:
                            dictionary["Target Database Type"] = ''
                        if dictionary["Target Connection ID"] == 'NULL' or dictionary["Target Connection ID"] is None:
                            dictionary["Target Connection ID"] = ''
                        if dictionary["Target Schema"] == 'NULL' or dictionary["Target Schema"] is None:
                            dictionary["Target Schema"] = ''
                        if dictionary["Target Type"] == 'NULL' or dictionary["Target Type"] is None:
                            dictionary["Target Type"] = ''

                        if dictionary['Transfer Source Data'] is False:
                            dictionary['Transfer Source Data'] = 'No'
                        else:
                            dictionary['Transfer Source Data'] = 'Yes'

                        projects.append(dictionary)

                    print(projects)
                except Exception as e:
                    flash(str(e), category="error")
        return self.render_template("project_change_tracking.html",
                                    projects=projects,
                                    count_projects=len(raw_projects))

    @expose("/add", methods=['GET', 'POST'])
    @csrf.exempt
    def project_add_data(self):
        """Add CT Project"""

        form = ProjectForm()

        if request.method == 'POST':

            form_add = ProjectForm(request.form)

            sql_insert_query = f"""
                                INSERT INTO airflow.atk_ct.ct_projects (
                                    source_database_type,
                                    source_connection_id,
                                    source_database,
                                    biview_database,
                                    project_database,
                                    biview_project_type,
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
                                    '{form_add.source_database.data}',
                                    '{form_add.biview_database.data}',
                                    '{form_add.project_database.data}',
                                    {form_add.biview_project_type.data},
                                    {form_add.transfer_source_data.data},
                                    {replace_response_data(form_add.target_database_type.data)},
                                    {replace_response_data(form_add.target_connection_id.data)},
                                    {replace_response_data(form_add.target_schema.data)},
                                    {replace_response_data(form_add.target_type.data)},
                                    {replace_response_datetime(form_add.update_dags_start_date.data)},
                                    {replace_response_datetime(form_add.update_dags_start_time.data)},
                                    '{form_add.update_dags_schedule.data}',
                                    {replace_response_datetime(form_add.transfer_dags_start_date.data)},
                                    {replace_response_datetime(form_add.transfer_dags_start_time.data)},
                                    '{form_add.transfer_dags_schedule.data}'
                                    );"""
            print(sql_insert_query)
            try:

                if form_add.source_database_type == " " or form_add.target_database_type == " ":
                    raise ValueError("Некорректное значение для типа базы данных!")

                with get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_insert_query)
                    conn.commit()

                flash("Проект успешно сохранен", category="info")
                return flask.redirect(url_for('ProjectsView.edit_project_data', project_database=form_add.project_database.data))
            except Exception as e:
                if 'duplicate key' in str(e):
                    flash("Данная база данных проекта уже существует! Выберите другую.", category='warning')
                else:
                    flash(str(e), category='warning')

        return self.render_template("add_projects.html", form=form)

    @expose("/edit/<string:project_database>", methods=['GET', 'POST'])
    @csrf.exempt
    def edit_project_data(self, project_database):
        """Edit of project data"""

        sql_select_query = f"""SELECT * FROM airflow.atk_ct.ct_projects WHERE project_database = '{project_database}';"""

        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select_query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                projects_data = [dict(zip(columns, row)) for row in rows]

        form_exist = ProjectForm(data=projects_data[0])

        form_update = ProjectForm(request.form)

        if request.method == 'POST':

            sql_update_query = f"""
                                UPDATE airflow.atk_ct.ct_projects
                                SET source_database = '{form_update.source_database.data}',
                                    biview_database = '{form_update.biview_database.data}',
                                    biview_project_type = {form_update.biview_project_type.data},
                                    transfer_source_data = {form_update.transfer_source_data.data},
                                    target_database_type = {replace_response_data(form_update.target_database_type.data)},
                                    target_connection_id = {replace_response_data(form_update.target_connection_id.data)},
                                    target_schema = {replace_response_data(form_update.target_schema.data)},
                                    target_type = {replace_response_data(form_update.target_type.data)},
                                    update_dags_start_date = {replace_response_datetime(
                form_update.update_dags_start_date.data)},
                                    update_dags_start_time = {replace_response_datetime(
                form_update.update_dags_start_time.data)},
                                    update_dags_schedule = '{form_update.update_dags_schedule.data}',
                                    transfer_dags_start_date = {replace_response_datetime(
                form_update.transfer_dags_start_date.data)},
                                    transfer_dags_start_time = {replace_response_datetime(
                form_update.transfer_dags_start_time.data)},
                                    transfer_dags_schedule = '{form_update.transfer_dags_schedule.data}'
                                WHERE project_database = '{project_database}'
                                ;"""
            print(sql_update_query)
            try:
                if form_update.source_database_type == " " or form_update.target_database_type == " ":
                    raise ValueError("Некорректное значение для типа базы данных!")
                with get_connection_postgres().get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_update_query)
                    conn.commit()
                flash("Проект успешно изменен", category="info")
                return flask.redirect(url_for('ProjectsView.edit_project_data', project_database=project_database))
            except Exception as e:
                if 'duplicate key' in str(e):
                    flash("Данное имя проекта уже существует! Выберите другое.", category='warning')
                elif 'None' in str(e):
                    flash("Введите дату и время!", category='warning')
                else:
                    flash(str(e), category='warning')

        return self.render_template("edit_project.html", form=form_exist)

    @expose('/projects_to_load', methods=['GET'])
    def projects_to_load(self):
        """Отображение списка таблиц"""
        project_database = request.args.get('project_database')
        connection = request.args.get('connection')
        source_database_type = request.args.get('source_database_type')
        return self.render_template("projects_to_load.html", project_database=project_database, connection=connection,
                                    source_database_type=source_database_type)

    @expose('/delete/<string:project_database>', methods=['GET'])
    @csrf.exempt
    def delete_ct_project(self, project_database):
        """Удалить проект"""
        sql_delete_query = """DELETE FROM airflow.atk_ct.ct_projects WHERE project_database = %s"""
        try:
            with get_connection_postgres().get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_delete_query, (project_database,))
                conn.commit()
            flash("Проект успешно удален", category="info")
        except Exception as e:
            flash(str(e))
        return flask.redirect(url_for('ProjectsView.project_list'))

    @expose('/api/get_connections/', methods=['GET'])
    def get_filtered_connections(self):
        """Функция возвращает список connections соответствующих принимаемому типу базы данных"""
        database_type = request.args.get('database_type')
        if not database_type:
            return jsonify({'status': 'error', 'message': 'No data provided'}), 400
        connections = GetConnection.get_database_connection(database_type)
        return jsonify(connections)

    @expose("/api/get_source_database/", methods=['GET'])
    def get_source_database(self):
        """Функция возвращает список баз данных соответствующих принимаемым connections"""

        databases = []
        get_connection = request.args.get('connection')
        print(get_connection)

        session = settings.Session()
        connections = session.query(Connection).all()
        connection = [conn for conn in connections if conn.conn_id == get_connection][0]

        if connection.conn_type == 'mssql':
            databases = GetDatabase.get_all_database_mssql(connection.conn_id)

        elif connection.conn_type == 'postgres':
            databases = GetDatabase.get_all_database_postgres(connection.conn_id)

        return jsonify(databases)

    @expose("/api/get_target_database/", methods=['GET'])
    def get_target_database(self):
        """Функция возвращает список баз данных соответствующих принимаемым connections"""

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

        project_database = request.args.get('project_database')
        print(project_database)
        sql_select_query = f"""SELECT * FROM airflow.atk_ct.ct_projects WHERE project_database = '{project_database}';"""

        with get_connection_postgres().get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_select_query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                print(rows)
                projects_data = [dict(zip(columns, row)) for row in rows][0]

        return jsonify(projects_data)

    @expose("/fetch_airflow_connections")
    @provide_session
    def fetch_airflow_connections(self, session=None):
        try:
            connections = session.query(Connection).all()
            connection_ids = [conn.conn_id for conn in connections]
            return jsonify({"status": "success", "connections": connection_ids})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})

    @expose("/fetch_data")
    def fetch_data(self):
        project_database = request.args.get('project_database')
        connection_id = request.args.get('connection')
        source_database_type = request.args.get('source_database_type')

        print("*" * 20)
        print("source_database_type: ", source_database_type)
        print("*" * 20)

        sql_query = f"""
                       SELECT
                           table_alias,
                           load
                       FROM {project_database}.dbo.ct__tables
                       WHERE exists_in_source = 1;
                    """
        print(sql_query)
        with get_hook_for_database(source_database_type, connection_id).get_conn() as conn:
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
        print(response_data)
        return jsonify(response_data)

    @expose("/update_data_is_load", methods=['POST'])
    @csrf.exempt
    def update_data_is_load(self):
        try:
            data = request.get_json()
            changes = data.get('data')

            connection_id = request.args.get('connection')
            source_database_type = request.args.get('source_database_type')
            project_database = request.args.get('project_database')

            if not data:
                return jsonify({'status': 'error', 'message': 'No data provided'}), 400

            with get_hook_for_database(source_database_type, connection_id).get_conn() as conn:
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

                                # SQL Injection Mitigation: Use placeholders for values
                                query = f"""
                                UPDATE {project_database}.dbo.ct__tables
                                SET {field} = %s
                                WHERE table_alias = %s;
                                """

                                update_queries.append((query, (new_value, table_alias)))

                        print("update_queries")
                        print(update_queries)
                        # Execute the queries
                        for query, params in update_queries:
                            cursor.execute(query, params)
                        conn.commit()
                        return jsonify({'status': 'success'}), 200

                    except Exception as e:
                        conn.rollback()  # Rollback in case of error
                        print(f"Error occurred while updating data: {e}")
                        return jsonify({'status': 'error', 'message': str(e)}), 500

        except Exception as e:
            print(f"Error processing request: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500


v_appbuilder_view = ProjectsView()
v_appbuilder_package = {
    "name": "Projects",
    "category": "Project Change Tracking",
    "view": v_appbuilder_view
}


class AirflowConnectionPlugin(AirflowPlugin):
    name = "project_list"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
