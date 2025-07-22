// lib/src/gossip_manager/pagination.dart

/// Represents a request for paginated results
class PageRequest {
  /// The page number (0-based)
  final int page;
  
  /// The number of items per page
  final int size;
  
  /// The offset calculated from page and size
  int get offset => page * size;

  const PageRequest({
    required this.page,
    required this.size,
  }) : assert(page >= 0, 'Page must be non-negative'),
       assert(size > 0, 'Size must be positive');

  /// Creates a PageRequest for the first page
  factory PageRequest.first(int size) {
    return PageRequest(page: 0, size: size);
  }

  /// Creates a PageRequest for the next page
  PageRequest next() {
    return PageRequest(page: page + 1, size: size);
  }

  /// Creates a PageRequest for the previous page
  PageRequest? previous() {
    if (page <= 0) return null;
    return PageRequest(page: page - 1, size: size);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is PageRequest &&
          runtimeType == other.runtimeType &&
          page == other.page &&
          size == other.size;

  @override
  int get hashCode => Object.hash(page, size);

  @override
  String toString() {
    return 'PageRequest{page: $page, size: $size, offset: $offset}';
  }
}

/// Represents a paginated result
class PageResult<T> {
  /// The content of this page
  final List<T> content;
  
  /// The page request that generated this result
  final PageRequest pageRequest;
  
  /// The total number of elements across all pages
  final int totalElements;
  
  /// The total number of pages
  final int totalPages;
  
  /// Whether this is the first page
  final bool isFirst;
  
  /// Whether this is the last page
  final bool isLast;
  
  /// Whether there is a next page
  final bool hasNext;
  
  /// Whether there is a previous page
  final bool hasPrevious;
  
  /// The number of elements in this page
  int get numberOfElements => content.length;
  
  /// The current page number (0-based)
  int get pageNumber => pageRequest.page;
  
  /// The page size
  int get pageSize => pageRequest.size;

  PageResult({
    required this.content,
    required this.pageRequest,
    required this.totalElements,
  }) : totalPages = totalElements == 0 ? 0 : ((totalElements - 1) ~/ pageRequest.size) + 1,
       isFirst = pageRequest.page == 0,
       isLast = pageRequest.page >= ((totalElements == 0 ? 0 : ((totalElements - 1) ~/ pageRequest.size))),
       hasNext = pageRequest.page < ((totalElements == 0 ? 0 : ((totalElements - 1) ~/ pageRequest.size))),
       hasPrevious = pageRequest.page > 0;

  /// Creates an empty page result
  factory PageResult.empty(PageRequest pageRequest) {
    return PageResult<T>(
      content: [],
      pageRequest: pageRequest,
      totalElements: 0,
    );
  }

  /// Creates a page result from a full list of items
  factory PageResult.fromList(List<T> allItems, PageRequest pageRequest) {
    final startIndex = pageRequest.offset;
    final endIndex = (startIndex + pageRequest.size).clamp(0, allItems.length);
    
    final content = startIndex < allItems.length 
        ? allItems.sublist(startIndex, endIndex)
        : <T>[];

    return PageResult<T>(
      content: content,
      pageRequest: pageRequest,
      totalElements: allItems.length,
    );
  }

  /// Maps the content of this page result to a different type
  PageResult<R> map<R>(R Function(T item) mapper) {
    return PageResult<R>(
      content: content.map(mapper).toList(),
      pageRequest: pageRequest,
      totalElements: totalElements,
    );
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is PageResult<T> &&
          runtimeType == other.runtimeType &&
          content == other.content &&
          pageRequest == other.pageRequest &&
          totalElements == other.totalElements;

  @override
  int get hashCode => Object.hash(content, pageRequest, totalElements);

  @override
  String toString() {
    return 'PageResult{page: ${pageRequest.page}, size: ${pageRequest.size}, '
           'totalElements: $totalElements, totalPages: $totalPages, '
           'numberOfElements: $numberOfElements, isFirst: $isFirst, isLast: $isLast}';
  }
}

/// Utility class for creating common page requests
class PageRequests {
  /// Creates a page request for the first page with default size
  static PageRequest first([int size = 20]) => PageRequest.first(size);
  
  /// Creates a page request for a specific page
  static PageRequest of(int page, int size) => PageRequest(page: page, size: size);
  
  /// Creates a page request with offset-based parameters
  static PageRequest fromOffset(int offset, int size) {
    final page = offset ~/ size;
    return PageRequest(page: page, size: size);
  }
}
