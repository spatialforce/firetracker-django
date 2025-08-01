from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

User = get_user_model()

class Command(BaseCommand):
    help = 'Custom superuser creation command'

    def handle(self, *args, **options):
        if User.objects.filter(is_superuser=True).exists():
            self.stdout.write(self.style.ERROR('A superuser already exists! Only one superuser is allowed.'))
            return

        # CHANGE THESE TO YOUR CREDENTIALS!
        username = "admin"  # change this
        email = "kudzanaichakavarika67@gmail.com"  # change this
        password = "rgmx sayx xjlg eein"  # change this

        User.objects.create_superuser(
            username=username,
            email=email,
            password=password
        )
        self.stdout.write(self.style.SUCCESS('Superuser created successfully!'))