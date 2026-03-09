#include "userfs.h"

#include "rlist.h"

#include <stddef.h>
#include <string>
#include <vector>
#include <cstring>

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char memory[BLOCK_SIZE];
	/** A link in the block list of the owner-file. */
	rlist in_block_list = RLIST_LINK_INITIALIZER;

	/* PUT HERE OTHER MEMBERS */
};

struct file {
	/**
	 * Doubly-linked intrusive list of file blocks. Intrusiveness of the
	 * list gives you the full control over the lifetime of the items in the
	 * list without having to use double pointers with performance penalty.
	 */
	rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
	/** How many file descriptors are opened on the file. */
	int refs = 0;
	/** File name. */
	std::string name;
	/** A link in the global file list. */
	rlist in_file_list = RLIST_LINK_INITIALIZER;

	/* PUT HERE OTHER MEMBERS */
	/** Total size of the file in bytes. */
	size_t size = 0;
	/** Number of blocks in the file. */
	size_t block_count = 0;
	/** Flag indicating if the file is deleted but still has open descriptors. */
	bool deleted = false;
};

/**
 * Intrusive list of all files. In this case the intrusiveness of the list also
 * grants the ability to remove items from any position in O(1) complexity
 * without having to know their iterator.
 */
static rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

struct filedesc {
	file *atfile;

	/* PUT HERE OTHER MEMBERS */
	/** Current position in the file. */
	size_t pos = 0;
	/** Cached block for sequential access optimization. */
	block *cached_block = nullptr;
	/** Index of the cached block. */
	size_t cached_block_idx = 0;
#if NEED_OPEN_FLAGS
	/** Permissions for this descriptor. */
	int permissions = UFS_READ_WRITE;
#endif
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static std::vector<filedesc*> file_descriptors;

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

/**
 * Find a file by name in the global file list.
 */
static file*
find_file(const char *filename)
{
	file *f;
	rlist_foreach_entry(f, &file_list, in_file_list) {
		if (!f->deleted && f->name == filename)
			return f;
	}
	return nullptr;
}

/**
 * Get the number of blocks needed for a given size.
 */
static size_t
get_block_count(size_t size)
{
	return (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
}


/**
 * Get block by index (0-based) with caching for sequential access.
 */
static block*
get_block_at_cached(file *f, size_t index, block **cached, size_t *cached_idx)
{
	// Check if we can use the cache
	if (*cached != nullptr && *cached_idx == index) {
		return *cached;
	}
	
	// Check if we can move forward from cache
	if (*cached != nullptr && *cached_idx < index) {
		block *b = *cached;
		size_t i = *cached_idx;
		while (i < index && !rlist_entry_is_head(b, &f->blocks, in_block_list)) {
			b = rlist_next_entry(b, in_block_list);
			i++;
			if (i == index) {
				*cached = b;
				*cached_idx = i;
				return b;
			}
		}
	}
	
	// Fall back to linear search from beginning
	size_t i = 0;
	block *b;
	rlist_foreach_entry(b, &f->blocks, in_block_list) {
		if (i == index) {
			*cached = b;
			*cached_idx = i;
			return b;
		}
		i++;
	}
	return nullptr;
}

/**
 * Free all blocks of a file.
 */
static void
free_file_blocks(file *f)
{
	block *b, *tmp;
	rlist_foreach_entry_safe(b, &f->blocks, in_block_list, tmp) {
		rlist_del_entry(b, in_block_list);
		delete b;
	}
	f->block_count = 0;
}

/**
 * Delete a file if it has no references.
 */
static void
try_delete_file(file *f)
{
	if (f->refs == 0 && f->deleted) {
		rlist_del_entry(f, in_file_list);
		free_file_blocks(f);
		delete f;
	}
}

int
ufs_open(const char *filename, int flags)
{
	ufs_error_code = UFS_ERR_NO_ERR;
	
	file *f = find_file(filename);
	
	if (f == nullptr) {
		if (!(flags & UFS_CREATE)) {
			ufs_error_code = UFS_ERR_NO_FILE;
			return -1;
		}
		// Create new file
		f = new file;
		f->name = filename;
		f->size = 0;
		f->block_count = 0;
		f->refs = 0;
		f->deleted = false;
		rlist_add_tail_entry(&file_list, f, in_file_list);
	}
	
	// Create file descriptor
	filedesc *fd = new filedesc;
	fd->atfile = f;
	fd->pos = 0;
	fd->cached_block = nullptr;
	fd->cached_block_idx = 0;
	
#if NEED_OPEN_FLAGS
	// Set permissions
	// Check for specific combinations first
	if ((flags & UFS_READ_WRITE) == UFS_READ_WRITE) {
		fd->permissions = UFS_READ_WRITE;
	} else if (flags & UFS_READ_ONLY) {
		fd->permissions = UFS_READ_ONLY;
	} else if (flags & UFS_WRITE_ONLY) {
		fd->permissions = UFS_WRITE_ONLY;
	} else {
		// Default: read-write
		fd->permissions = UFS_READ_WRITE;
	}
#endif
	
	f->refs++;
	
	// Find free slot in file_descriptors
	for (size_t i = 0; i < file_descriptors.size(); i++) {
		if (file_descriptors[i] == nullptr) {
			file_descriptors[i] = fd;
			return i;
		}
	}
	
	// No free slot, add to end
	file_descriptors.push_back(fd);
	return file_descriptors.size() - 1;
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
	ufs_error_code = UFS_ERR_NO_ERR;
	
	if (fd < 0 || (size_t)fd >= file_descriptors.size() ||
	    file_descriptors[fd] == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
	filedesc *desc = file_descriptors[fd];
	
#if NEED_OPEN_FLAGS
	if ((desc->permissions & UFS_WRITE_ONLY) == 0) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	
	file *f = desc->atfile;
	
	// Check if we would exceed max file size
	size_t new_end = desc->pos + size;
	if (new_end > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	
	// Ensure we have enough blocks
	size_t blocks_needed = get_block_count(new_end);
	while (f->block_count < blocks_needed) {
		block *b = new block;
		rlist_add_tail_entry(&f->blocks, b, in_block_list);
		f->block_count++;
	}
	
	size_t written = 0;
	
	while (written < size) {
		size_t block_idx = (desc->pos + written) / BLOCK_SIZE;
		size_t offset_in_block = (desc->pos + written) % BLOCK_SIZE;
		size_t to_write = BLOCK_SIZE - offset_in_block;
		if (to_write > size - written)
			to_write = size - written;
		
		// Get block (it must exist now) with caching
		block *b = get_block_at_cached(f, block_idx, &desc->cached_block,
		                                &desc->cached_block_idx);
		
		// Write data
		memcpy(b->memory + offset_in_block, buf + written, to_write);
		written += to_write;
	}
	
	desc->pos += written;
	
	// Update file size if we wrote past the end
	if (desc->pos > f->size)
		f->size = desc->pos;
	
	return written;
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
	ufs_error_code = UFS_ERR_NO_ERR;
	
	if (fd < 0 || (size_t)fd >= file_descriptors.size() || 
	    file_descriptors[fd] == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
	filedesc *desc = file_descriptors[fd];
	
#if NEED_OPEN_FLAGS
	if ((desc->permissions & UFS_READ_ONLY) == 0) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	
	file *f = desc->atfile;
	
	// Check if we're at or past EOF
	if (desc->pos >= f->size)
		return 0;
	
	// Adjust size to not read past EOF
	size_t available = f->size - desc->pos;
	if (size > available)
		size = available;
	
	size_t read_bytes = 0;
	
	while (read_bytes < size) {
		size_t block_idx = (desc->pos + read_bytes) / BLOCK_SIZE;
		size_t offset_in_block = (desc->pos + read_bytes) % BLOCK_SIZE;
		size_t to_read = BLOCK_SIZE - offset_in_block;
		if (to_read > size - read_bytes)
			to_read = size - read_bytes;
		
		block *b = get_block_at_cached(f, block_idx, &desc->cached_block,
		                                &desc->cached_block_idx);
		if (b == nullptr) {
			// This shouldn't happen if file size is correct
			break;
		}
		
		// Read data
		memcpy(buf + read_bytes, b->memory + offset_in_block, to_read);
		read_bytes += to_read;
	}
	
	desc->pos += read_bytes;
	
	return read_bytes;
}

int
ufs_close(int fd)
{
	ufs_error_code = UFS_ERR_NO_ERR;
	
	if (fd < 0 || (size_t)fd >= file_descriptors.size() || 
	    file_descriptors[fd] == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
	filedesc *desc = file_descriptors[fd];
	file *f = desc->atfile;
	
	f->refs--;
	
	delete desc;
	file_descriptors[fd] = nullptr;
	
	// Try to delete file if it was marked for deletion
	try_delete_file(f);
	
	return 0;
}

int
ufs_delete(const char *filename)
{
	ufs_error_code = UFS_ERR_NO_ERR;
	
	file *f = find_file(filename);
	if (f == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
	f->deleted = true;
	
	// Try to delete immediately if no references
	try_delete_file(f);
	
	return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
	ufs_error_code = UFS_ERR_NO_ERR;
	
	if (fd < 0 || (size_t)fd >= file_descriptors.size() || 
	    file_descriptors[fd] == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	
	filedesc *desc = file_descriptors[fd];
	
#if NEED_OPEN_FLAGS
	if ((desc->permissions & UFS_WRITE_ONLY) == 0) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	
	file *f = desc->atfile;
	
	if (new_size > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	
	size_t old_size = f->size;
	size_t new_blocks = get_block_count(new_size);
	
	if (new_size > old_size) {
		// Expand: add blocks
		while (f->block_count < new_blocks) {
			block *b = new block;
			memset(b->memory, 0, BLOCK_SIZE);
			rlist_add_tail_entry(&f->blocks, b, in_block_list);
			f->block_count++;
		}
	} else if (new_size < old_size) {
		// Shrink: remove blocks
		while (f->block_count > new_blocks) {
			if (rlist_empty(&f->blocks))
				break;
			block *b = rlist_last_entry(&f->blocks, block, in_block_list);
			rlist_del_entry(b, in_block_list);
			delete b;
			f->block_count--;
		}
		
		// Adjust file descriptor positions that are beyond new size
		for (size_t i = 0; i < file_descriptors.size(); i++) {
			if (file_descriptors[i] != nullptr && 
			    file_descriptors[i]->atfile == f &&
			    file_descriptors[i]->pos > new_size) {
				file_descriptors[i]->pos = new_size;
			}
		}
	}
	
	f->size = new_size;
	
	return 0;
}

#endif

void
ufs_destroy(void)
{
	// Close all file descriptors
	for (size_t i = 0; i < file_descriptors.size(); i++) {
		if (file_descriptors[i] != nullptr) {
			delete file_descriptors[i];
			file_descriptors[i] = nullptr;
		}
	}
	
	// Clear the vector properly to free memory
	std::vector<filedesc*>().swap(file_descriptors);
	
	// Delete all files
	file *f, *tmp;
	rlist_foreach_entry_safe(f, &file_list, in_file_list, tmp) {
		rlist_del_entry(f, in_file_list);
		free_file_blocks(f);
		delete f;
	}
}
