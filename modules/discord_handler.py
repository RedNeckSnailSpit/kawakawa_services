import discord
from discord import app_commands
from discord.ext import commands

from modules.database_handler import DatabaseHandler

# --- DiscordHandler class ---
class DiscordHandler:
    def __init__(self):
        # Initialize DB and retrieve token
        self.db = DatabaseHandler()
        raw_token = self.db.get_setting('discord_token')
        if raw_token:
            token = raw_token.decode('utf-8')
        else:
            token = input("🔑  Enter your Discord bot token: ").strip()
            self.db.upsert_setting('discord_token', token.encode('utf-8'))
        self.token = token

        # Permission check for admin-only commands
        def is_authorized_user():
            async def predicate(interaction: discord.Interaction) -> bool:
                return interaction.user.guild_permissions.administrator
            return app_commands.check(predicate)

        # Configure bot and tree
        intents = discord.Intents.default()
        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self.tree = self.bot.tree

        # Global error handler for slash commands
        @self.tree.error
        async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
            if isinstance(error, app_commands.CheckFailure):
                await interaction.response.send_message(
                    "🚫 You do not have permission to use this command.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    f"⚠️ An unexpected error occurred: {error}",
                    ephemeral=True
                )

        # --- Basic Commands ---

        @self.tree.command(name="ping", description="Replies with Pong!")
        async def ping(interaction: discord.Interaction):
            await interaction.response.send_message("Pong!")

        @self.tree.command(name="help", description="List available commands and usage")
        async def help_command(interaction: discord.Interaction):
            user = interaction.user
            is_admin = interaction.guild is not None and user.guild_permissions.administrator

            help_text = "**Available Commands:**\n"
            help_text += "- `/help` — Displays this message\n"
            help_text += "- `/ping` — Replies with 'Pong!'\n"

            if is_admin:
                help_text += "\n**Admin Commands:**\n"
                help_text += "- `None yet!`\n"

            await interaction.response.send_message(help_text, ephemeral=True)

    def run(self):
        @self.bot.event
        async def on_ready():
            await self.tree.sync()
            print(f"✅  Logged in as {self.bot.user} (ID: {self.bot.user.id})")

        self.bot.run(self.token)
