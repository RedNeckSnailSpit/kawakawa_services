from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

from modules.database_handler import DatabaseHandler


class TelegramHandler:
    def __init__(self):
        # Initialize DB and retrieve token
        self.db = DatabaseHandler()
        raw_token = self.db.get_setting('telegram_token')
        if raw_token:
            token = raw_token.decode('utf-8')
        else:
            token = input("🔑  Enter your Telegram bot token: ").strip()
            self.db.upsert_setting('telegram_token', token.encode('utf-8'))
        self.token = token

        # Create application
        self.application = Application.builder().token(self.token).build()

        # Add handlers
        self._add_handlers()

    def _add_handlers(self):
        """Add command handlers to the application."""
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("ping", self.ping))
        self.application.add_handler(CommandHandler("help", self.help_command))

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /start command."""
        await update.message.reply_text("👋 Hello! I'm your bot. Use /help to see available commands.")

    async def ping(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /ping command."""
        await update.message.reply_text("Pong!")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /help command."""
        help_text = "*Available Commands:*\n"
        help_text += "• /start — Start the bot\n"
        help_text += "• /help — Show this message\n"
        help_text += "• /ping — Replies with 'Pong!'\n"

        await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

    def run(self):
        """Start the bot."""
        print(f"✅  Starting Telegram bot...")
        self.application.run_polling()