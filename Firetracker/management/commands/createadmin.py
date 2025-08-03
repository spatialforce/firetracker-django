from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
import os

class Command(BaseCommand):
    help = 'Creates a superuser from environment variables'

    def handle(self, *args, **options):
        username = os.getenv('ADMIN_USERNAME')
        email = os.getenv('ADMIN_EMAIL')
        password = os.getenv('ADMIN_PASSWORD')
        
        if not all([username, email, password]):
            self.stdout.write(self.style.ERROR('Missing admin credentials in environment variables'))
            return
            
        if User.objects.filter(username=username).exists():
            self.stdout.write(self.style.WARNING('Admin user already exists'))
            return
            
        User.objects.create_superuser(
            username=username,
            email=email,
            password=password
        )
        self.stdout.write(self.style.SUCCESS('Superuser created successfully'))