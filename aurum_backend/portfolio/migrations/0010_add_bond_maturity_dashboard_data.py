# Generated manually for bond maturity dashboard feature

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('portfolio', '0009_add_bank_allocation_data'),
    ]

    operations = [
        migrations.AddField(
            model_name='dateaggregatedmetrics',
            name='bond_maturity_data',
            field=models.JSONField(default=dict, help_text='Aggregated bond maturity distribution for this date'),
        ),
    ]