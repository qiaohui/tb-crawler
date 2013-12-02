#coding:utf-8
import re
from django.db import models
from django.contrib.auth.models import User
from django.contrib import admin
from django.contrib.admin import SimpleListFilter

class Shop(models.Model):
    """shop model"""
    url = models.URLField()
    name = models.CharField(max_length=64)
    is_cloth = models.BooleanField(default=False)
    is_voted = models.BooleanField(default=False)
    assigned_to =  models.ForeignKey(User, null=True)
    is_delete = models.BooleanField(default=False)

    def url_link(self):
        return '<a target="_blank" href="%s">%s</a>' % (self.url, self.url)
    url_link.allow_tags = True

class UserVotedFilter(SimpleListFilter):
    title = "我的filter"
    parameter_name = "mine"

    def lookups(self, request, model_admin):
        return (('no', '没处理的'),
                ('yes', '处理过的'),
        )

    def queryset(self, request, queryset):
        if self.value() == 'no':
            return queryset.filter(assigned_to = request.user).filter(is_delete=0).filter(is_voted = 0)
        if self.value() == 'yes':
            return queryset.filter(assigned_to = request.user).filter(is_delete=0).filter(is_voted = 1)
        if not request.user.is_superuser:
            return queryset.filter(assigned_to = request.user)
        else:
            return queryset

class ShopAdmin(admin.ModelAdmin):
    save_on_top = True
    list_per_page = 20
    list_filter = (UserVotedFilter,)
    list_display = ('name', 'url_link', 'is_cloth')
    list_editable = ('is_cloth',)
    fields = ('name', 'url', 'is_cloth', 'assigned_to', 'is_voted')

    def changelist_view(self, request):
        results = admin.ModelAdmin.changelist_view(self, request)
        if request.POST.has_key("_save"):
            self.process_save_list(request.POST.dict())
        return results
    
    def process_save_list(self, data):
        #import pdb; pdb.set_trace()
        FORMID_RE = re.compile('form-[0-9]+-id')
        submit_ids = [int(v) for k,v in data.items() if FORMID_RE.match(k)]
        for id in submit_ids:
            shop = Shop.objects.get(pk=id)
            shop.is_voted = True
            shop.save()

admin.site.register(Shop, ShopAdmin)

