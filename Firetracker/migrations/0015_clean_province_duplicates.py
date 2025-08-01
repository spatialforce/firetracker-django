from django.db import migrations

def clean_province_duplicates(apps, schema_editor):
    Province = apps.get_model('Firetracker', 'Province')
    
    # Find all duplicate admin1Pcod values
    from django.db.models import Count, Min
    duplicates = (
        Province.objects.values('admin1Pcod')
        .annotate(count=Count('id'), min_id=Min('id'))
        .filter(count__gt=1)
    )
    
    for dup in duplicates:
        # Delete all duplicates except the one with the lowest ID
        Province.objects.filter(
            admin1Pcod=dup['admin1Pcod']
        ).exclude(
            id=dup['min_id']
        ).delete()

class Migration(migrations.Migration):
    dependencies = [
        # Replace with your actual previous migration
        ('Firetracker', '0014_auto_20250801_1339'),  
    ]

    operations = [
        migrations.RunPython(clean_province_duplicates, migrations.RunPython.noop),
    ]