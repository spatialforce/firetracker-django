from django.contrib import admin
from django.contrib.gis import admin as gis_admin
from django.urls import path
from django.shortcuts import render, redirect
from django.contrib import messages
from django.db import transaction
from django import forms
from django.utils import timezone
from django.conf import settings
from .models import Province, District, FirePoint, GeoDataUpload
from .forms import GeoDataUploadForm
import logging
import os

logger = logging.getLogger(__name__)

class ProvinceAdmin(gis_admin.GISModelAdmin):
    list_display = ('admin1Name', 'admin1Pcod', 'created_at', 'updated_at')
    search_fields = ('admin1Name', 'admin1Pcod')
    list_filter = ('created_at', 'updated_at')
    readonly_fields = ('created_at', 'updated_at')
    ordering = ('admin1Name',)

    def created_at(self, obj):
        return obj.created_at.strftime('%Y-%m-%d %H:%M') if obj.created_at else 'N/A'
    created_at.short_description = 'Created At'

    def updated_at(self, obj):
        return obj.updated_at.strftime('%Y-%m-%d %H:%M') if obj.updated_at else 'N/A'
    updated_at.short_description = 'Updated At'

class DistrictAdmin(gis_admin.GISModelAdmin):
    list_display = ('admin2Name', 'admin2Pcod', 'admin1Name', 'created_at', 'updated_at')
    search_fields = ('admin2Name', 'admin1Name')
    list_filter = ('admin1Name', 'created_at')
    readonly_fields = ('created_at', 'updated_at')
    ordering = ('admin1Name', 'admin2Name')

    def created_at(self, obj):
        return obj.created_at.strftime('%Y-%m-%d %H:%M') if obj.created_at else 'N/A'
    created_at.short_description = 'Created At'

    def updated_at(self, obj):
        return obj.updated_at.strftime('%Y-%m-%d %H:%M') if obj.updated_at else 'N/A'
    updated_at.short_description = 'Updated At'

class FirePointAdmin(gis_admin.GISModelAdmin):
    list_display = ('acq_date', 'latitude', 'longitude', 'confidence', 'frp', 'created_at')
    list_filter = ('acq_date', 'confidence', 'created_at')
    date_hierarchy = 'acq_date'
    search_fields = ('latitude', 'longitude')
    readonly_fields = ('created_at', 'updated_at')
    ordering = ('-acq_date',)
    actions = ['delete_selected_bulk']

    def created_at(self, obj):
        return obj.created_at.strftime('%Y-%m-%d %H:%M') if obj.created_at else 'N/A'
    created_at.short_description = 'Created At'

    def updated_at(self, obj):
        return obj.updated_at.strftime('%Y-%m-%d %H:%M') if obj.updated_at else 'N/A'
    updated_at.short_description = 'Updated At'

    @admin.action(description='Delete selected firepoints (bulk)')
    def delete_selected_bulk(self, request, queryset):
        try:
            with transaction.atomic():
                count = queryset.count()
                batch_size = 1000
                for i in range(0, count, batch_size):
                    batch = queryset[i:i+batch_size]
                    batch._raw_delete(batch.db)
                self.message_user(
                    request,
                    f'Successfully deleted {count} firepoints',
                    messages.SUCCESS
                )
        except Exception as e:
            self.message_user(
                request,
                f'Error deleting firepoints: {str(e)}',
                messages.ERROR
            )

class GeoDataUploadAdminForm(forms.ModelForm):
    class Meta:
        model = GeoDataUpload
        fields = '__all__'
        widgets = {
            'data_type': forms.Select(attrs={
                'onchange': "updateFormatField(this);"
            }),
        }

class GeoDataUploadAdmin(admin.ModelAdmin):
    form = GeoDataUploadAdminForm
    list_display = ('title', 'data_type', 'upload_format', 'processed', 'records_processed', 'processing_status', 'processing_errors_short')
    readonly_fields = ('processing_errors_display', 'created_at', 'updated_at', 'processing_time', 'processing_status')
    list_filter = ('data_type', 'upload_format', 'processed', 'created_at')
    actions = ['process_selected', 'retry_failed_uploads']
    date_hierarchy = 'created_at'
    
    def processing_errors_short(self, obj):
        return obj.processing_errors[:100] + "..." if obj.processing_errors else ""
    processing_errors_short.short_description = 'Errors'
    
    def processing_errors_display(self, obj):
        return obj.processing_errors or "No errors"
    processing_errors_display.short_description = 'Processing Errors'
    
    def processing_status(self, obj):
        if obj.processed:
            return "✅ Completed"
        return "⏳ Pending"
    processing_status.short_description = 'Status'

    def processing_time(self, obj):
        if obj.processing_time:
            return f"{obj.processing_time.total_seconds():.2f} sec"
        return "N/A"
    processing_time.short_description = 'Processing Time'

    def created_at(self, obj):
        return obj.created_at.strftime('%Y-%m-%d %H:%M') if obj.created_at else 'N/A'
    created_at.short_description = 'Created At'

    def updated_at(self, obj):
        return obj.updated_at.strftime('%Y-%m-%d %H:%M') if obj.updated_at else 'N/A'
    updated_at.short_description = 'Updated At'

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('upload/', self.admin_site.admin_view(self.upload_view), name='geodata_upload'),
        ]
        return custom_urls + urls

    def upload_view(self, request):
        if request.method == 'POST':
            form = GeoDataUploadForm(request.POST, request.FILES)
            if form.is_valid():
                upload = form.save(commit=False)
                try:
                    with transaction.atomic():
                        upload.save()  # Save first to get ID
                        if upload.process():
                            messages.success(
                                request,
                                f'Successfully processed {upload.records_processed} records from {upload.title}'
                            )
                        else:
                            messages.error(
                                request,
                                f'Processing failed: {upload.processing_errors}'
                            )
                    return redirect('admin:your_app_geodataupload_changelist')
                except Exception as e:
                    messages.error(request, f'System error: {str(e)}')
        else:
            form = GeoDataUploadForm()
        
        return render(request, 'admin/geodata_upload.html', {
            'form': form,
            'title': 'Upload Geographic Data',
            'opts': self.model._meta,
        })

    @admin.action(description='Process selected uploads')
    def process_selected(self, request, queryset):
        success_count = 0
        failure_count = 0
        
        for upload in queryset:
            try:
                logger.info(f"Processing upload {upload.id} - {upload.title}")
                if upload.process():
                    success_count += 1
                    self.message_user(
                        request,
                        f'Successfully processed {upload.title} ({upload.records_processed} records)',
                        messages.SUCCESS
                    )
                else:
                    failure_count += 1
                    self.message_user(
                        request,
                        f'Failed to process {upload.title}: {upload.processing_errors}',
                        messages.ERROR
                    )
            except Exception as e:
                failure_count += 1
                self.message_user(
                    request,
                    f'System error processing {upload.title}: {str(e)}',
                    messages.ERROR
                )
        
        self.message_user(
            request,
            f'Processing complete. Success: {success_count}, Failures: {failure_count}',
            messages.SUCCESS if success_count > 0 else messages.ERROR
        )

    @admin.action(description='Retry failed uploads')
    def retry_failed_uploads(self, request, queryset):
        queryset = queryset.filter(processed=False)
        self.process_selected(request, queryset)

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        logger.info(f"Saved GeoDataUpload {obj.id} by {request.user}")

admin.site.register(Province, ProvinceAdmin)
admin.site.register(District, DistrictAdmin)
admin.site.register(FirePoint, FirePointAdmin)
admin.site.register(GeoDataUpload, GeoDataUploadAdmin)