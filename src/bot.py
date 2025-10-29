import os
import tweepy
from dotenv import load_dotenv
from api_scraper import ApiScraper
from logger_setup import setup_logging
import logging

def run_bot():
    """
    Runs the scraper and posts new health inspection scores to Twitter.
    """
    setup_logging()
    # Load environment variables from .env file
    load_dotenv()

    # --- Run the scraper ---
    scraper = ApiScraper()
    new_inspections = scraper.run()

    if not new_inspections:
        logging.info("No new inspections found to tweet.")
        return

    logging.info(f"Found {len(new_inspections)} new inspections. Posting to Twitter...")

    # --- Authenticate with Twitter ---
    # Make sure to set these in your .env file or as environment variables
    consumer_key = os.environ.get("TWITTER_CONSUMER_KEY")
    consumer_secret = os.environ.get("TWITTER_CONSUMER_SECRET")
    access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
    access_token_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")

    if not all([consumer_key, consumer_secret, access_token, access_token_secret]):
        logging.info("Twitter API keys not found. Skipping tweet.")
        return

    client = tweepy.Client(
        consumer_key=consumer_key, consumer_secret=consumer_secret,
        access_token=access_token, access_token_secret=access_token_secret
    )

    # --- Post tweets ---
    for inspection in new_inspections:
        tweet_text = f"✅ Health Score: {inspection['name']} scored a {inspection['score']} on {inspection['date']}."
        try:
            client.create_tweet(text=tweet_text)
            logging.info(f"  Posted tweet for {inspection['name']}")
        except tweepy.errors.TweepyException as e:
            logging.error(f"  Error posting tweet for {inspection['name']}: {e}")

if __name__ == "__main__":
    run_bot()