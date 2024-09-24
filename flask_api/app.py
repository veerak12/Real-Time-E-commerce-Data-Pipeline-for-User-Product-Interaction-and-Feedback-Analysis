from flask import Flask, jsonify
from sqlalchemy import create_engine, text
from faker import Faker
import random
import uuid
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

app = Flask(__name__)
fake = Faker()

# Configure logging
logging.basicConfig(filename='user_activity.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create an engine to connect to your MySQL database
engine = create_engine("mysql+pymysql://:@mysql-veera--920a.f.aivencloud.com:25250/")

def get_product_and_category_ids():
    try:
        with engine.connect() as connection:
            product_result = connection.execute(text("SELECT product_id FROM products"))
            category_result = connection.execute(text("SELECT category_id FROM categories"))
            product_ids = [row['product_id'] for row in product_result.mappings()]
            category_ids = [row['category_id'] for row in category_result.mappings()]
        return product_ids, category_ids
    except Exception as e:
        logging.error(f"Error in get_product_and_category_ids: {e}")
        return [], []

def get_user_data():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT user_id, first_name, last_name FROM users"))
            users = [(row['user_id'], row['first_name'], row['last_name']) for row in result.mappings()]
        return users
    except Exception as e:
        logging.error(f"Error in get_user_data: {e}")
        return []

def format_datetime(dt):
    if dt is None:
        return None
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def generate_user_activity():
    product_ids, category_ids = get_product_and_category_ids()
    users = get_user_data()

    if not product_ids or not users:
        logging.error("No product IDs or user data available in the database")
        return None

    user_id, first_name, last_name = random.choice(users)
    if random.random() < 0.7: # 70% chance to create a new user
        user_id = str(uuid.uuid4())
        first_name = fake.first_name()
        last_name = fake.last_name()
        registration_date = format_datetime(fake.date_this_year())
    else:
        registration_date = None

    activity = {
        'timestamp': format_datetime(fake.date_time_this_year()),
        'user_id': user_id,
        'first_name': first_name,
        'last_name': last_name,
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'address': fake.address(),
        'date_of_birth': format_datetime(fake.date_of_birth(minimum_age=18, maximum_age=90)),
        'gender': random.choice(['Male', 'Female', 'Non-binary', 'Prefer not to say']),
        'nationality': fake.country(),
        'credit_card_number': fake.credit_card_number(),
        'credit_card_expiry': fake.credit_card_expire(),
        'bank_account_number': fake.bban(),
        'annual_income': random.randint(20000, 150000),
        'product_id': random.choice(product_ids),
        'category_id': random.choice(category_ids),
        'transaction_amount': round(random.uniform(1.0, 1000.0), 2),
        'transaction_date': format_datetime(fake.date_time_this_year()),
        'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
        'job_title': fake.job(),
        'company_name': fake.company(),
        'start_date': format_datetime(fake.date_this_decade()),
        'username': fake.user_name(),
        'website_url': fake.url(),
        'twitter_handle': fake.user_name(),
        'customer_review': fake.text(),
        'feedback': fake.sentence(),
        'latitude': fake.latitude(),
        'longitude': fake.longitude(),
        'country': fake.country(),
        'city': fake.city(),
        'created_at': format_datetime(fake.date_time_this_year()),
        'updated_at': format_datetime(fake.date_time_this_year()),
        'last_login': format_datetime(fake.date_time_this_month()),
        'subscription_type': random.choice(['Basic', 'Premium', 'Enterprise']),
        'customer_status': random.choice(['Active', 'Inactive', 'Pending']),
        'session_duration': random.randint(1, 1800),
        'user_rating': round(random.uniform(1.0, 5.0), 1)
    }

    logging.info(f"Generated user activity: {activity}")
    return activity

@app.route('/api/v1/user-activity', methods=['GET'])
def api_generate_user_activity():
    activity = generate_user_activity()
    if activity is None:
        return jsonify({"error": "Failed to generate user activity"}), 500
    return jsonify(activity)

# @app.route('/api/v1/user-activity', methods=['GET'])
# def api_generate_user_activity():
#     """API endpoint to generate and return multiple user activities."""
#     # Generate a list of user activities, for example, 5 records
#     num_records = 5  # You can modify this number
#     activities = [generate_user_activity() for _ in range(num_records)]

#     # Check if any activity generation failed
#     if not activities:
#         return jsonify({"error": "Failed to generate user activities"}), 500

#     return jsonify(activities)

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(generate_user_activity, 'interval', minutes=1)
    scheduler.start()
    logging.info("Scheduler started and job set to run every 1 minutes.")
    app.run(debug=True, port=5000)
  