from datetime import datetime

from airflow.models import DAG

from airflow.operators.dummy_operator import DummyOperator

with DAG(dag_id="customer-journey-pipeline",
         start_date=datetime(2021, 12, 10),
         schedule_interval="@daily") as dag:
    # SFDC data processing
    download_opportunities = DummyOperator(task_id="download-sfdc-opportunities")
    download_conversations = DummyOperator(task_id="download-sfdc-conversations")
    download_companies = DummyOperator(task_id="download-sfdc-companies")
    download_contacts = DummyOperator(task_id="download-sfdc-contacts")
    download_sales_agents = DummyOperator(task_id="download-sales-agents")

    # Application data processing
    download_application_users = DummyOperator(task_id="download-application-users")
    download_user_actions = DummyOperator(task_id="download-user-actions")

    # Zendesk data processing
    download_zendesk_customers = DummyOperator(task_id="download-zendesk-customers")
    download_zendesk_tickets = DummyOperator(task_id="download-zendesk-tickets")
    download_zendesk_conversations = DummyOperator(task_id="download-zendesk-conversations")

    # Matching
    match_sfdc_contacts_to_app_users = DummyOperator(task_id="match-sfdc-contacts-to-app-users")
    match_app_users_to_zendesk_customers = DummyOperator(task_id="match-app-users-to-zendesk-customers")

    # Create customer journey data structure
    create_customer_journey = DummyOperator(task_id="create-customer-journey")

    [download_contacts, download_application_users] >> match_sfdc_contacts_to_app_users
    [download_application_users, download_zendesk_customers] >> match_app_users_to_zendesk_customers

    [download_opportunities, download_conversations, download_companies, download_contacts, download_sales_agents,
     download_application_users, download_user_actions, download_zendesk_conversations, download_zendesk_tickets,
     download_zendesk_customers, match_sfdc_contacts_to_app_users,
     match_app_users_to_zendesk_customers] >> create_customer_journey
