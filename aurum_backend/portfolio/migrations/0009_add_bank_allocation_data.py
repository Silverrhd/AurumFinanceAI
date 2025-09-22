# Generated manually for bank allocation feature

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('portfolio', '0008_fix_dashboard_cache_implementation'),
    ]

    operations = [
        migrations.AddField(
            model_name='dateaggregatedmetrics',
            name='bank_allocation_data',
            field=models.JSONField(default=dict, help_text='Aggregated bank allocation for this date'),
        ),
    ]