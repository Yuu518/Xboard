<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('v2_server_traffic_cursor', function (Blueprint $table) {
            $table->unsignedBigInteger('node_id');
            $table->uuid('stream');
            $table->unsignedBigInteger('sequence');
            $table->primary(['node_id', 'stream']);
        });
        Schema::create('v2_server_traffic_pending', function (Blueprint $table) {
            $table->id();
            $table->longText('payload');
            $table->unsignedBigInteger('created_at')->index();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('v2_server_traffic_pending');
        Schema::dropIfExists('v2_server_traffic_cursor');
    }
};
